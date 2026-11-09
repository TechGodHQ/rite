//! Pure durable watch state and dual-clock recovery transitions.
//!
//! Persisted UTC instants use signed Unix milliseconds. Active monotonic time
//! is an unsigned millisecond offset from one process-local anchor and is
//! never serialized. Filesystem, host-clock acquisition, HTTP, and tasks remain
//! server-owned.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{RiteAction, RiteEvent, RiteWatch, Severity};

/// Durable state format version.
pub const WATCH_STATE_VERSION: u32 = 1;
/// One-second ticks are at most one quarter of this minimum window.
pub const MIN_WATCH_WINDOW_SECS: u64 = 4;
/// Watch-only request deadline in milliseconds.
pub const ATTEMPT_TIMEOUT_MILLIS: i64 = 10_000;
/// Maximum durably consumed reservations for one logical firing.
pub const MAX_ATTEMPTS: u8 = 3;
const MILLIS_PER_SECOND: u64 = 1_000;

/// Core transition failure; messages deliberately contain no config or event data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum WatchError {
    /// The durable record is unsupported, malformed, or internally inconsistent.
    #[error("watch state record is invalid")]
    InvalidRecord,
    /// A valid saved obligation has no usable comparable host recovery time.
    #[error("comparable host recovery time is unavailable")]
    HostTimeUnavailable,
    /// A checked UTC/monotonic conversion or state transition overflowed.
    #[error("watch clock arithmetic overflow")]
    ClockOverflow,
    /// A requested state transition is inconsistent with the committed phase.
    #[error("watch state transition is invalid")]
    InvalidTransition,
    /// A synthetic firing timestamp cannot be represented by the supported UTC type.
    #[error("watch timestamp is outside the supported range")]
    TimestampOutOfRange,
}

/// A persisted versioned snapshot. All timestamps are UTC Unix milliseconds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchFile {
    /// Durable schema version.
    pub version: u32,
    /// Nondecreasing highest committed logical UTC observation.
    pub high_water_utc_ms: i64,
    /// UTC instant of the last durable replacement.
    pub written_at_utc_ms: i64,
    /// Name-keyed active and retained watch records.
    pub watches: BTreeMap<String, WatchRecord>,
}

impl WatchFile {
    /// Create a validated empty snapshot for a newly configured watch service.
    pub fn empty(now_utc_ms: i64) -> Result<Self, WatchError> {
        if !supported_utc_millis(now_utc_ms) {
            return Err(WatchError::TimestampOutOfRange);
        }
        Ok(Self {
            version: WATCH_STATE_VERSION,
            high_water_utc_ms: now_utc_ms,
            written_at_utc_ms: now_utc_ms,
            watches: BTreeMap::new(),
        })
    }

    /// Validate the durable schema and all phase/deadline invariants.
    pub fn validate(&self) -> Result<(), WatchError> {
        if self.version != WATCH_STATE_VERSION
            || !supported_utc_millis(self.high_water_utc_ms)
            || !supported_utc_millis(self.written_at_utc_ms)
            || self.written_at_utc_ms > self.high_water_utc_ms
        {
            return Err(WatchError::InvalidRecord);
        }
        for (name, watch) in &self.watches {
            watch.validate(name, self.high_water_utc_ms)?;
        }
        Ok(())
    }

    /// Commit a logical observation without permitting durable time to move back.
    pub fn advance_time(&mut self, now_utc_ms: i64) -> Result<(), WatchError> {
        if now_utc_ms < self.high_water_utc_ms || !supported_utc_millis(now_utc_ms) {
            return Err(WatchError::HostTimeUnavailable);
        }
        self.high_water_utc_ms = now_utc_ms;
        self.written_at_utc_ms = now_utc_ms;
        Ok(())
    }
}

/// Durable record for one watch name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchRecord {
    /// Last allocated per-watch firing sequence.
    pub sequence: u64,
    /// Most recent admitted heartbeat time; null until one has matched.
    pub last_seen_utc_ms: Option<i64>,
    /// Start of the currently armed silence interval.
    pub arm_started_utc_ms: i64,
    /// Window used by the current arm; later config changes apply on next arm.
    pub window_secs: u64,
    /// Unchanged absolute UTC boundary for the current silence interval.
    pub silence_deadline_utc_ms: i64,
    /// Creation time of the latest logical firing, if any.
    pub last_fired_utc_ms: Option<i64>,
    /// One nonterminal firing, if one exists.
    pub pending: Option<FiringRecord>,
    /// Most recent terminal outcome, retained across subsequent arms.
    pub last_outcome: Option<TerminalOutcome>,
}

impl WatchRecord {
    /// Arm a genuinely new watch without inventing a heartbeat observation.
    pub fn new(now_utc_ms: i64, window_secs: u64) -> Result<Self, WatchError> {
        let deadline = add_window(now_utc_ms, window_secs)?;
        Ok(Self {
            sequence: 0,
            last_seen_utc_ms: None,
            arm_started_utc_ms: now_utc_ms,
            window_secs,
            silence_deadline_utc_ms: deadline,
            last_fired_utc_ms: None,
            pending: None,
            last_outcome: None,
        })
    }

    /// Validate invariants independent of the current host time.
    pub fn validate(&self, name: &str, high_water_utc_ms: i64) -> Result<(), WatchError> {
        if name.trim().is_empty()
            || self.window_secs < MIN_WATCH_WINDOW_SECS
            || !supported_utc_millis(self.arm_started_utc_ms)
            || !supported_utc_millis(self.silence_deadline_utc_ms)
            || self.arm_started_utc_ms > high_water_utc_ms
            || add_window(self.arm_started_utc_ms, self.window_secs)?
                != self.silence_deadline_utc_ms
            || self.last_seen_utc_ms.is_some_and(|time| {
                !supported_utc_millis(time)
                    || time > high_water_utc_ms
                    || time > self.arm_started_utc_ms
            })
            || self
                .last_fired_utc_ms
                .is_some_and(|time| !supported_utc_millis(time) || time > high_water_utc_ms)
        {
            return Err(WatchError::InvalidRecord);
        }
        if let Some(pending) = &self.pending {
            pending.validate(name, self.sequence, high_water_utc_ms)?;
        }
        if let Some(outcome) = &self.last_outcome {
            outcome.validate(self.sequence, high_water_utc_ms)?;
        }
        Ok(())
    }

    /// Apply an admitted heartbeat to the next silence interval only.
    pub fn observe_heartbeat(
        &mut self,
        now_utc_ms: i64,
        window_secs: u64,
    ) -> Result<(), WatchError> {
        let deadline = add_window(now_utc_ms, window_secs)?;
        self.last_seen_utc_ms = Some(now_utc_ms);
        self.arm_started_utc_ms = now_utc_ms;
        self.window_secs = window_secs;
        self.silence_deadline_utc_ms = deadline;
        Ok(())
    }

    /// Create one coalesced firing when the silence boundary is due.
    pub fn fire_if_due(
        &mut self,
        name: &str,
        now_utc_ms: i64,
        window_secs: u64,
        action_fingerprint: &str,
    ) -> Result<bool, WatchError> {
        if self.pending.is_some() || now_utc_ms < self.silence_deadline_utc_ms {
            return Ok(false);
        }
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(WatchError::ClockOverflow)?;
        let deadline = add_window(now_utc_ms, window_secs)?;
        let firing_id = format!("{name}:{sequence}");
        let timestamp = DateTime::<Utc>::from_timestamp_millis(now_utc_ms)
            .ok_or(WatchError::TimestampOutOfRange)?;
        let event = RiteEvent {
            source: "monitor".into(),
            event_type: "deadline_exceeded".into(),
            action: None,
            timestamp,
            severity: Severity::Warning,
            title: format!("Watch {name} deadline exceeded"),
            body: None,
            metadata: BTreeMap::from([
                ("watch_name".into(), json!(name)),
                ("firing_id".into(), json!(firing_id)),
            ]),
        };
        self.sequence = sequence;
        self.arm_started_utc_ms = now_utc_ms;
        self.window_secs = window_secs;
        self.silence_deadline_utc_ms = deadline;
        self.last_fired_utc_ms = Some(now_utc_ms);
        self.pending = Some(FiringRecord {
            id: firing_id,
            sequence,
            event,
            created_at_utc_ms: now_utc_ms,
            action_fingerprint: action_fingerprint.to_owned(),
            attempts_reserved: 0,
            uncertain: false,
            phase: AttemptPhase::Ready {
                eligible_utc_ms: now_utc_ms,
            },
        });
        Ok(true)
    }

    /// Durably consume one reservation before a caller performs network I/O.
    pub fn reserve_attempt(&mut self, now_utc_ms: i64) -> Result<Option<FiringRecord>, WatchError> {
        let Some(firing) = &mut self.pending else {
            return Ok(None);
        };
        let eligible = match firing.phase {
            AttemptPhase::Ready { eligible_utc_ms }
            | AttemptPhase::Waiting {
                retry_at_utc_ms: eligible_utc_ms,
                ..
            }
            | AttemptPhase::Attempting {
                recovery_at_utc_ms: eligible_utc_ms,
                ..
            } => eligible_utc_ms,
        };
        if now_utc_ms < eligible {
            return Ok(None);
        }
        if firing.attempts_reserved >= MAX_ATTEMPTS {
            return Err(WatchError::InvalidTransition);
        }
        let ordinal = firing
            .attempts_reserved
            .checked_add(1)
            .ok_or(WatchError::ClockOverflow)?;
        let delay = retry_delay_millis(ordinal).ok_or(WatchError::InvalidTransition)?;
        let recovery_at_utc_ms = now_utc_ms
            .checked_add(ATTEMPT_TIMEOUT_MILLIS)
            .and_then(|time| time.checked_add(delay))
            .ok_or(WatchError::ClockOverflow)?;
        firing.attempts_reserved = ordinal;
        firing.phase = AttemptPhase::Attempting {
            reserved_at_utc_ms: now_utc_ms,
            recovery_at_utc_ms,
        };
        Ok(Some(firing.clone()))
    }

    /// Commit an observed action result after the send has completed or timed out.
    pub fn complete_attempt(
        &mut self,
        now_utc_ms: i64,
        acknowledged: bool,
    ) -> Result<(), WatchError> {
        let firing = self.pending.as_ref().ok_or(WatchError::InvalidTransition)?;
        if !matches!(firing.phase, AttemptPhase::Attempting { .. }) {
            return Err(WatchError::InvalidTransition);
        }
        let mut firing = self.pending.take().ok_or(WatchError::InvalidTransition)?;
        if acknowledged {
            self.last_outcome = Some(TerminalOutcome {
                firing_id: firing.id,
                sequence: firing.sequence,
                finished_at_utc_ms: now_utc_ms,
                attempts_reserved: firing.attempts_reserved,
                result: TerminalResult::Accepted,
                uncertain: firing.uncertain,
            });
        } else {
            firing.uncertain = true;
            if firing.attempts_reserved >= MAX_ATTEMPTS {
                self.last_outcome = Some(TerminalOutcome {
                    firing_id: firing.id,
                    sequence: firing.sequence,
                    finished_at_utc_ms: now_utc_ms,
                    attempts_reserved: firing.attempts_reserved,
                    result: TerminalResult::RetryExhausted,
                    uncertain: true,
                });
            } else {
                let delay = retry_delay_millis(firing.attempts_reserved)
                    .ok_or(WatchError::InvalidTransition)?;
                let retry_at_utc_ms = now_utc_ms
                    .checked_add(delay)
                    .ok_or(WatchError::ClockOverflow)?;
                firing.phase = AttemptPhase::Waiting {
                    outcome_at_utc_ms: now_utc_ms,
                    retry_at_utc_ms,
                };
                self.pending = Some(firing);
            }
        }
        Ok(())
    }

    /// Terminalize an uncertain final reservation after its persisted deadline.
    pub fn exhaust_crashed_final_attempt(&mut self, now_utc_ms: i64) -> Result<bool, WatchError> {
        let Some(firing) = &self.pending else {
            return Ok(false);
        };
        let AttemptPhase::Attempting {
            recovery_at_utc_ms, ..
        } = firing.phase
        else {
            return Ok(false);
        };
        if firing.attempts_reserved != MAX_ATTEMPTS || now_utc_ms < recovery_at_utc_ms {
            return Ok(false);
        }
        let firing = self.pending.take().ok_or(WatchError::InvalidTransition)?;
        self.last_outcome = Some(TerminalOutcome {
            firing_id: firing.id,
            sequence: firing.sequence,
            finished_at_utc_ms: now_utc_ms,
            attempts_reserved: firing.attempts_reserved,
            result: TerminalResult::RetryExhausted,
            uncertain: true,
        });
        Ok(true)
    }

    /// Terminalize an explicitly removed watch without fabricating acceptance.
    pub fn remove_with_pending(&mut self, now_utc_ms: i64) -> Result<bool, WatchError> {
        let Some(firing) = self.pending.take() else {
            return Ok(false);
        };
        let uncertain = firing.uncertain || firing.attempts_reserved > 0;
        self.last_outcome = Some(TerminalOutcome {
            firing_id: firing.id,
            sequence: firing.sequence,
            finished_at_utc_ms: now_utc_ms,
            attempts_reserved: firing.attempts_reserved,
            result: TerminalResult::Removed,
            uncertain,
        });
        Ok(true)
    }
}

/// Durable event/action identity and finite attempt state for one firing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FiringRecord {
    /// Stable watch-name/sequence identity shared by all attempts.
    pub id: String,
    /// Monotonic sequence within this watch.
    pub sequence: u64,
    /// Synthetic event snapshot; never contains the source heartbeat body.
    pub event: RiteEvent,
    /// Logical creation instant, also used as the event timestamp.
    pub created_at_utc_ms: i64,
    /// SHA-256 reference to the configured generic action; no credentials are stored.
    pub action_fingerprint: String,
    /// Number of durably consumed attempt reservations.
    pub attempts_reserved: u8,
    /// True if any attempt may have caused a remote effect.
    pub uncertain: bool,
    /// Persisted next-attempt/recovery phase.
    pub phase: AttemptPhase,
}

impl FiringRecord {
    fn validate(
        &self,
        name: &str,
        sequence: u64,
        high_water_utc_ms: i64,
    ) -> Result<(), WatchError> {
        if self.sequence == 0
            || self.sequence > sequence
            || self.id != format!("{name}:{}", self.sequence)
            || !supported_utc_millis(self.created_at_utc_ms)
            || self.created_at_utc_ms > high_water_utc_ms
            || self.event.timestamp.timestamp_millis() != self.created_at_utc_ms
            || self.event.source != "monitor"
            || self.event.event_type != "deadline_exceeded"
            || self.event.action.is_some()
            || self.event.severity != Severity::Warning
            || self.event.body.is_some()
            || self.event.metadata.len() != 2
            || self.event.metadata.get("watch_name") != Some(&json!(name))
            || self.event.metadata.get("firing_id") != Some(&json!(self.id))
            || !valid_fingerprint(&self.action_fingerprint)
            || self.attempts_reserved > MAX_ATTEMPTS
            || !self.phase.timestamps_supported()
        {
            return Err(WatchError::InvalidRecord);
        }
        match self.phase {
            AttemptPhase::Ready { eligible_utc_ms }
                if self.attempts_reserved == 0 && eligible_utc_ms == self.created_at_utc_ms =>
            {
                Ok(())
            }
            AttemptPhase::Attempting {
                reserved_at_utc_ms,
                recovery_at_utc_ms,
            } if self.attempts_reserved > 0
                && reserved_at_utc_ms >= self.created_at_utc_ms
                && reserved_at_utc_ms <= high_water_utc_ms
                && expected_crash_recovery_at(reserved_at_utc_ms, self.attempts_reserved)?
                    == recovery_at_utc_ms =>
            {
                Ok(())
            }
            AttemptPhase::Waiting {
                outcome_at_utc_ms,
                retry_at_utc_ms,
            } if (1..MAX_ATTEMPTS).contains(&self.attempts_reserved)
                && outcome_at_utc_ms >= self.created_at_utc_ms
                && outcome_at_utc_ms <= high_water_utc_ms
                && outcome_at_utc_ms.checked_add(
                    retry_delay_millis(self.attempts_reserved).ok_or(WatchError::InvalidRecord)?,
                ) == Some(retry_at_utc_ms) =>
            {
                Ok(())
            }
            _ => Err(WatchError::InvalidRecord),
        }
    }
}

/// Persisted attempt/retry phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum AttemptPhase {
    /// Intent is durable but no reservation has yet been consumed.
    Ready { eligible_utc_ms: i64 },
    /// Reservation is durable; after a crash, this boundary includes the local
    /// request timeout and the ordinal-specific retry delay.
    Attempting {
        reserved_at_utc_ms: i64,
        recovery_at_utc_ms: i64,
    },
    /// A completed non-acknowledged result is durably waiting for its retry.
    Waiting {
        outcome_at_utc_ms: i64,
        retry_at_utc_ms: i64,
    },
}

impl AttemptPhase {
    fn timestamps_supported(self) -> bool {
        match self {
            Self::Ready { eligible_utc_ms } => supported_utc_millis(eligible_utc_ms),
            Self::Attempting {
                reserved_at_utc_ms,
                recovery_at_utc_ms,
            } => {
                supported_utc_millis(reserved_at_utc_ms) && supported_utc_millis(recovery_at_utc_ms)
            }
            Self::Waiting {
                outcome_at_utc_ms,
                retry_at_utc_ms,
            } => supported_utc_millis(outcome_at_utc_ms) && supported_utc_millis(retry_at_utc_ms),
        }
    }
}

/// Outcome retained after a firing becomes terminal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalOutcome {
    /// Identity of the completed logical firing.
    pub firing_id: String,
    /// Per-watch sequence of that firing.
    pub sequence: u64,
    /// Logical terminalization/acknowledgement instant.
    pub finished_at_utc_ms: i64,
    /// Number of consumed reservations; never refunded.
    pub attempts_reserved: u8,
    /// Terminal classification.
    pub result: TerminalResult,
    /// Possible duplicate/remote-acceptance risk retained across later success.
    pub uncertain: bool,
}

impl TerminalOutcome {
    fn validate(&self, sequence: u64, high_water_utc_ms: i64) -> Result<(), WatchError> {
        if self.sequence == 0
            || self.sequence > sequence
            || !supported_utc_millis(self.finished_at_utc_ms)
            || self.finished_at_utc_ms > high_water_utc_ms
            || self.attempts_reserved > MAX_ATTEMPTS
            || (self.result == TerminalResult::Accepted && self.attempts_reserved == 0)
            || (self.result == TerminalResult::RetryExhausted
                && self.attempts_reserved != MAX_ATTEMPTS)
        {
            return Err(WatchError::InvalidRecord);
        }
        Ok(())
    }
}

/// Finite terminal classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalResult {
    /// A 2xx response status was durably recorded.
    Accepted,
    /// All three reservations ended without a durably recorded 2xx status.
    RetryExhausted,
    /// The watch was removed while its firing was still pending.
    Removed,
}

/// UTC and monotonic clock values sampled for one recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryClock {
    /// Host UTC in the durable millisecond scale, if available and representable.
    pub host_utc_ms: Option<i64>,
    /// Active monotonic epoch anchor, if available.
    pub monotonic_anchor_ms: Option<u64>,
    /// The documented environmental host-time assumption, not a detection of arbitrary clock manipulation.
    pub host_time_trusted: bool,
}

/// One-time mapping of persisted boundaries into the current monotonic epoch.
#[derive(Debug, Clone, PartialEq)]
pub struct RecoveredWatch {
    /// Validated durable state, with an in-flight reservation marked uncertain.
    pub record: WatchRecord,
    /// Current monotonic eligibility corresponding to the unchanged UTC silence deadline.
    pub silence_deadline_mono_ms: u64,
    /// Current monotonic retry/recovery eligibility for a pending firing.
    pub firing_eligible_mono_ms: Option<u64>,
}

/// Recovered durable state plus one process-local UTC/monotonic mapping.
#[derive(Debug, Clone, PartialEq)]
pub struct RecoveredFile {
    /// UTC recovery sample paired once with this process's monotonic anchor.
    pub epoch: ClockEpoch,
    /// Name-keyed records with their one-time mapped boundaries.
    pub watches: BTreeMap<String, RecoveredWatch>,
}

/// Active-epoch mapping: T(M) = W + (M - M0).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockEpoch {
    /// Recovery host UTC sample W.
    pub utc_origin_ms: i64,
    /// Monotonic anchor M0.
    pub monotonic_origin_ms: u64,
}

impl ClockEpoch {
    /// Derive durable logical time from the same active epoch, never a new wall sample.
    pub fn utc_at(self, monotonic_ms: u64) -> Result<i64, WatchError> {
        let elapsed = monotonic_ms
            .checked_sub(self.monotonic_origin_ms)
            .ok_or(WatchError::ClockOverflow)?;
        let elapsed = i64::try_from(elapsed).map_err(|_| WatchError::ClockOverflow)?;
        let utc_ms = self
            .utc_origin_ms
            .checked_add(elapsed)
            .ok_or(WatchError::ClockOverflow)?;
        if !supported_utc_millis(utc_ms) {
            return Err(WatchError::TimestampOutOfRange);
        }
        Ok(utc_ms)
    }

    /// Map one unchanged absolute UTC boundary into this process epoch.
    pub fn map_boundary(self, boundary_utc_ms: i64) -> Result<u64, WatchError> {
        if !supported_utc_millis(boundary_utc_ms) {
            return Err(WatchError::TimestampOutOfRange);
        }
        if boundary_utc_ms <= self.utc_origin_ms {
            return Ok(self.monotonic_origin_ms);
        }
        let delta = boundary_utc_ms
            .checked_sub(self.utc_origin_ms)
            .ok_or(WatchError::ClockOverflow)?;
        let delta = u64::try_from(delta).map_err(|_| WatchError::ClockOverflow)?;
        self.monotonic_origin_ms
            .checked_add(delta)
            .ok_or(WatchError::ClockOverflow)
    }
}

/// Validate state before assessing whether the current host clock is comparable.
pub fn recover(file: &WatchFile, clock: RecoveryClock) -> Result<RecoveredFile, WatchError> {
    file.validate()?;
    let host_utc_ms = clock
        .host_utc_ms
        .filter(|time| *time >= file.high_water_utc_ms && supported_utc_millis(*time))
        .filter(|_| clock.host_time_trusted)
        .ok_or(WatchError::HostTimeUnavailable)?;
    let monotonic_anchor_ms = clock
        .monotonic_anchor_ms
        .ok_or(WatchError::HostTimeUnavailable)?;
    let epoch = ClockEpoch {
        utc_origin_ms: host_utc_ms,
        monotonic_origin_ms: monotonic_anchor_ms,
    };
    let mut watches = BTreeMap::new();
    for (name, record) in &file.watches {
        let mut record = record.clone();
        let silence_deadline_mono_ms = epoch.map_boundary(record.silence_deadline_utc_ms)?;
        let firing_eligible_mono_ms = if let Some(firing) = &mut record.pending {
            let boundary = match firing.phase {
                AttemptPhase::Ready { eligible_utc_ms } => eligible_utc_ms,
                AttemptPhase::Attempting {
                    recovery_at_utc_ms, ..
                } => {
                    firing.uncertain = true;
                    recovery_at_utc_ms
                }
                AttemptPhase::Waiting {
                    retry_at_utc_ms, ..
                } => {
                    firing.uncertain = true;
                    retry_at_utc_ms
                }
            };
            Some(epoch.map_boundary(boundary)?)
        } else {
            None
        };
        watches.insert(
            name.clone(),
            RecoveredWatch {
                record,
                silence_deadline_mono_ms,
                firing_eligible_mono_ms,
            },
        );
    }
    Ok(RecoveredFile { epoch, watches })
}

/// Stable lowercase SHA-256 identity for an action; the action and any credentials are never persisted.
pub fn action_fingerprint(action: &RiteAction) -> Result<String, WatchError> {
    let encoded = serde_json::to_vec(action).map_err(|_| WatchError::InvalidRecord)?;
    let digest = Sha256::digest(encoded);
    let mut result = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut result, "{byte:02x}").map_err(|_| WatchError::InvalidRecord)?;
    }
    Ok(result)
}

/// Retry delay after a completed outcome; final reservation has no further retry.
#[must_use]
pub const fn retry_delay_millis(ordinal: u8) -> Option<i64> {
    match ordinal {
        1 => Some(5_000),
        2 => Some(30_000),
        3 => Some(0),
        _ => None,
    }
}

/// Validate a watch window and return its exact durable millisecond width.
pub fn window_millis(window_secs: u64) -> Result<i64, WatchError> {
    if window_secs < MIN_WATCH_WINDOW_SECS {
        return Err(WatchError::InvalidRecord);
    }
    let millis = window_secs
        .checked_mul(MILLIS_PER_SECOND)
        .filter(|millis| i64::try_from(*millis).is_ok())
        .ok_or(WatchError::ClockOverflow)?;
    i64::try_from(millis).map_err(|_| WatchError::ClockOverflow)
}

/// Create an initial arm for a newly configured name without fabricating `last_seen`.
pub fn new_record_for_config(
    watch: &RiteWatch,
    now_utc_ms: i64,
) -> Result<WatchRecord, WatchError> {
    WatchRecord::new(now_utc_ms, watch.window_secs)
}

fn add_window(now_utc_ms: i64, window_secs: u64) -> Result<i64, WatchError> {
    if !supported_utc_millis(now_utc_ms) {
        return Err(WatchError::TimestampOutOfRange);
    }
    let deadline = now_utc_ms
        .checked_add(window_millis(window_secs)?)
        .ok_or(WatchError::ClockOverflow)?;
    if !supported_utc_millis(deadline) {
        return Err(WatchError::TimestampOutOfRange);
    }
    Ok(deadline)
}

fn supported_utc_millis(value: i64) -> bool {
    DateTime::<Utc>::from_timestamp_millis(value).is_some()
}

fn expected_crash_recovery_at(reserved_at_utc_ms: i64, ordinal: u8) -> Result<i64, WatchError> {
    reserved_at_utc_ms
        .checked_add(ATTEMPT_TIMEOUT_MILLIS)
        .and_then(|time| time.checked_add(retry_delay_millis(ordinal)?))
        .ok_or(WatchError::ClockOverflow)
}

fn valid_fingerprint(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RiteAction;
    use url::Url;

    fn action() -> RiteAction {
        RiteAction::HttpPost {
            url: Url::parse("http://127.0.0.1:9/action").expect("synthetic URL parses"),
            headers: BTreeMap::new(),
            body_template: None,
            body_json: None,
        }
    }

    fn fingerprint() -> String {
        action_fingerprint(&action()).expect("action fingerprints")
    }

    fn record() -> WatchRecord {
        WatchRecord::new(0, 60).expect("initial watch arms")
    }

    fn due_firing(record: &mut WatchRecord) {
        assert!(
            record
                .fire_if_due("watch", 60_000, 60, &fingerprint())
                .expect("due watch fires")
        );
    }

    #[test]
    fn initial_arm_is_never_seen_and_survives_a_mid_window_reopen() {
        let record = record();
        assert_eq!(record.last_seen_utc_ms, None);
        assert_eq!(record.silence_deadline_utc_ms, 60_000);
        let file = WatchFile {
            version: WATCH_STATE_VERSION,
            high_water_utc_ms: 0,
            written_at_utc_ms: 0,
            watches: BTreeMap::from([("watch".into(), record)]),
        };
        let reopened: WatchFile =
            serde_json::from_slice(&serde_json::to_vec(&file).expect("state serializes"))
                .expect("durable state reopens");
        let mapped = recover(
            &reopened,
            RecoveryClock {
                host_utc_ms: Some(20_000),
                monotonic_anchor_ms: Some(700),
                host_time_trusted: true,
            },
        )
        .expect("valid recovery mapping");
        assert_eq!(
            mapped.watches["watch"].silence_deadline_mono_ms, 40_700,
            "the original UTC boundary maps once; it is not reset to boot plus 60s"
        );
        assert_eq!(mapped.watches["watch"].record.last_seen_utc_ms, None);
    }

    #[test]
    fn overdue_silence_coalesces_to_one_stable_firing_and_full_window() {
        let mut record = record();
        assert!(
            record
                .fire_if_due("watch", 120_000, 60, &fingerprint())
                .expect("long outage creates one due firing")
        );
        let firing_id = record.pending.as_ref().expect("firing persists").id.clone();
        assert_eq!(record.silence_deadline_utc_ms, 180_000);
        assert!(
            !record
                .fire_if_due("watch", 120_001, 60, &fingerprint())
                .expect("pending firing coalesces overdue periods")
        );
        assert_eq!(record.pending.as_ref().unwrap().id, firing_id);
    }

    #[test]
    fn heartbeat_during_pending_backoff_resets_only_the_future_deadline() {
        let mut record = record();
        due_firing(&mut record);
        let _ = record
            .reserve_attempt(60_000)
            .expect("first reservation persists");
        record
            .complete_attempt(61_000, false)
            .expect("uncertain outcome schedules retry");
        let firing_before = record.pending.clone().expect("retry remains pending");
        record
            .observe_heartbeat(62_000, 120)
            .expect("heartbeat rearms future silence window");
        assert_eq!(record.silence_deadline_utc_ms, 182_000);
        assert_eq!(record.last_seen_utc_ms, Some(62_000));
        assert_eq!(record.pending, Some(firing_before));
        assert!(record.pending.as_ref().unwrap().uncertain);
    }

    #[test]
    fn observed_retry_wait_and_crash_recovery_boundaries_are_absolute() {
        let mut record = record();
        due_firing(&mut record);
        record.reserve_attempt(60_000).unwrap();
        record.complete_attempt(61_000, false).unwrap();
        assert_eq!(
            record.pending.as_ref().unwrap().phase,
            AttemptPhase::Waiting {
                outcome_at_utc_ms: 61_000,
                retry_at_utc_ms: 66_000,
            }
        );
        let reserved = record.reserve_attempt(66_000).unwrap().unwrap();
        assert_eq!(reserved.attempts_reserved, 2);
        assert_eq!(
            reserved.phase,
            AttemptPhase::Attempting {
                reserved_at_utc_ms: 66_000,
                recovery_at_utc_ms: 106_000,
            }
        );
        let file = WatchFile {
            version: WATCH_STATE_VERSION,
            high_water_utc_ms: 66_000,
            written_at_utc_ms: 66_000,
            watches: BTreeMap::from([("watch".into(), record)]),
        };
        let recovered = recover(
            &file,
            RecoveryClock {
                host_utc_ms: Some(70_000),
                monotonic_anchor_ms: Some(10),
                host_time_trusted: true,
            },
        )
        .expect("crash recovery maps the saved boundary");
        assert_eq!(
            recovered.watches["watch"].firing_eligible_mono_ms,
            Some(36_010),
            "retry eligibility stays reservation plus 10s plus ordinal-two delay"
        );
        assert!(
            recovered.watches["watch"]
                .record
                .pending
                .as_ref()
                .unwrap()
                .uncertain
        );
    }

    #[test]
    fn each_crash_ordinal_keeps_its_recovery_boundary_across_repeated_boots() {
        for ordinal in 1..=MAX_ATTEMPTS {
            let reserved_at = i64::from(ordinal) * 100_000;
            let recovery_at = expected_crash_recovery_at(reserved_at, ordinal).unwrap();
            let mut record = record();
            record.sequence = 1;
            let event_time = 0;
            let event = RiteEvent {
                source: "monitor".into(),
                event_type: "deadline_exceeded".into(),
                action: None,
                timestamp: DateTime::<Utc>::from_timestamp_millis(event_time).unwrap(),
                severity: Severity::Warning,
                title: "Watch watch deadline exceeded".into(),
                body: None,
                metadata: BTreeMap::from([
                    ("watch_name".into(), json!("watch")),
                    ("firing_id".into(), json!("watch:1")),
                ]),
            };
            record.pending = Some(FiringRecord {
                id: "watch:1".into(),
                sequence: 1,
                event,
                created_at_utc_ms: 0,
                action_fingerprint: fingerprint(),
                attempts_reserved: ordinal,
                uncertain: ordinal > 1,
                phase: AttemptPhase::Attempting {
                    reserved_at_utc_ms: reserved_at,
                    recovery_at_utc_ms: recovery_at,
                },
            });
            record.last_fired_utc_ms = Some(0);
            let file = WatchFile {
                version: WATCH_STATE_VERSION,
                high_water_utc_ms: reserved_at,
                written_at_utc_ms: reserved_at,
                watches: BTreeMap::from([("watch".into(), record)]),
            };
            let first_boot = recovery_at.saturating_sub(2_000);
            let first = recover(
                &file,
                RecoveryClock {
                    host_utc_ms: Some(first_boot),
                    monotonic_anchor_ms: Some(5),
                    host_time_trusted: true,
                },
            )
            .expect("first recovery is comparable");
            assert_eq!(first.watches["watch"].firing_eligible_mono_ms, Some(2_005));
            let second_boot = recovery_at.saturating_sub(500);
            let second = recover(
                &file,
                RecoveryClock {
                    host_utc_ms: Some(second_boot.max(reserved_at)),
                    monotonic_anchor_ms: Some(900),
                    host_time_trusted: true,
                },
            )
            .expect("repeated recovery is comparable");
            assert_eq!(
                second.watches["watch"].firing_eligible_mono_ms,
                Some(if second_boot < reserved_at {
                    900 + u64::try_from(recovery_at - reserved_at).unwrap()
                } else {
                    1_400
                })
            );
        }
    }

    #[test]
    fn valid_record_and_unusable_host_clock_are_distinct_and_leave_bytes_unchanged() {
        let record = record();
        let file = WatchFile {
            version: WATCH_STATE_VERSION,
            high_water_utc_ms: 10_000,
            written_at_utc_ms: 10_000,
            watches: BTreeMap::from([("watch".into(), record)]),
        };
        file.validate().expect("record is internally valid");
        let before = serde_json::to_vec(&file).unwrap();
        for clock in [
            RecoveryClock {
                host_utc_ms: None,
                monotonic_anchor_ms: Some(0),
                host_time_trusted: true,
            },
            RecoveryClock {
                host_utc_ms: Some(9_999),
                monotonic_anchor_ms: Some(0),
                host_time_trusted: true,
            },
            RecoveryClock {
                host_utc_ms: Some(10_000),
                monotonic_anchor_ms: Some(0),
                host_time_trusted: false,
            },
            RecoveryClock {
                host_utc_ms: Some(10_000),
                monotonic_anchor_ms: None,
                host_time_trusted: true,
            },
        ] {
            assert_eq!(recover(&file, clock), Err(WatchError::HostTimeUnavailable));
            assert_eq!(serde_json::to_vec(&file).unwrap(), before);
        }
        let mut invalid = file.clone();
        invalid
            .watches
            .get_mut("watch")
            .unwrap()
            .silence_deadline_utc_ms += 1;
        assert_eq!(
            recover(
                &invalid,
                RecoveryClock {
                    host_utc_ms: None,
                    monotonic_anchor_ms: Some(0),
                    host_time_trusted: false,
                }
            ),
            Err(WatchError::InvalidRecord),
            "invalid durable bytes are classified before host-clock comparability"
        );
    }

    #[test]
    fn recovery_rejects_host_utc_outside_supported_event_timestamp_range() {
        let file = WatchFile {
            version: WATCH_STATE_VERSION,
            high_water_utc_ms: 10_000,
            written_at_utc_ms: 10_000,
            watches: BTreeMap::from([("watch".into(), record())]),
        };
        let before = serde_json::to_vec(&file).unwrap();

        assert_eq!(
            recover(
                &file,
                RecoveryClock {
                    host_utc_ms: Some(i64::MAX),
                    monotonic_anchor_ms: Some(0),
                    host_time_trusted: true,
                }
            ),
            Err(WatchError::HostTimeUnavailable)
        );
        assert_eq!(serde_json::to_vec(&file).unwrap(), before);
    }

    #[test]
    fn accepted_after_uncertainty_suppresses_replay_but_retains_duplicate_risk() {
        let mut record = record();
        due_firing(&mut record);
        record.reserve_attempt(60_000).unwrap();
        record.complete_attempt(61_000, false).unwrap();
        record.reserve_attempt(66_000).unwrap();
        record.complete_attempt(67_000, true).unwrap();
        assert!(record.pending.is_none());
        let outcome = record.last_outcome.unwrap();
        assert_eq!(outcome.result, TerminalResult::Accepted);
        assert!(outcome.uncertain);
        assert_eq!(outcome.attempts_reserved, 2);
    }

    #[test]
    fn final_uncertain_attempt_exhausts_without_a_fourth_reservation() {
        let mut record = record();
        due_firing(&mut record);
        record.reserve_attempt(60_000).unwrap();
        record.complete_attempt(60_001, false).unwrap();
        record.reserve_attempt(65_001).unwrap();
        record.complete_attempt(65_002, false).unwrap();
        record.reserve_attempt(95_002).unwrap();
        record.complete_attempt(95_003, false).unwrap();
        assert!(record.pending.is_none());
        let outcome = record.last_outcome.unwrap();
        assert_eq!(outcome.result, TerminalResult::RetryExhausted);
        assert_eq!(outcome.attempts_reserved, MAX_ATTEMPTS);
        assert!(outcome.uncertain);
    }

    #[test]
    fn both_equal_time_orders_are_explicit_and_heartbeat_never_cancels_intent() {
        let mut heartbeat_first = record();
        heartbeat_first.observe_heartbeat(60_000, 60).unwrap();
        assert!(
            !heartbeat_first
                .fire_if_due("watch", 60_000, 60, &fingerprint())
                .unwrap()
        );

        let mut firing_first = record();
        firing_first
            .fire_if_due("watch", 60_000, 60, &fingerprint())
            .unwrap();
        let firing_id = firing_first.pending.as_ref().unwrap().id.clone();
        firing_first.observe_heartbeat(60_000, 60).unwrap();
        assert_eq!(firing_first.pending.as_ref().unwrap().id, firing_id);
        assert_eq!(firing_first.silence_deadline_utc_ms, 120_000);
    }

    #[test]
    fn clock_mappings_use_checked_arithmetic() {
        let epoch = ClockEpoch {
            utc_origin_ms: i64::MAX - 1,
            monotonic_origin_ms: 0,
        };
        assert_eq!(epoch.utc_at(2), Err(WatchError::ClockOverflow));
        let epoch = ClockEpoch {
            utc_origin_ms: 0,
            monotonic_origin_ms: u64::MAX,
        };
        assert_eq!(epoch.map_boundary(1), Err(WatchError::ClockOverflow));
        assert_eq!(epoch.utc_at(u64::MAX - 1), Err(WatchError::ClockOverflow));
    }
}
