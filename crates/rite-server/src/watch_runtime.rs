//! Server-owned durable watch coordinator and action execution.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use rite_core::{
    RiteAction, RiteEvent, RiteWatch,
    watch::{
        ATTEMPT_TIMEOUT_MILLIS, ClockEpoch, FiringRecord, RecoveredFile, RecoveryClock, WatchError,
        WatchFile, WatchRecord, action_fingerprint, recover,
    },
};
use thiserror::Error;
use tokio::time::timeout;

use crate::http_post_request;

const EVALUATION_INTERVAL: Duration = Duration::from_secs(1);
const ACTION_TIMEOUT: Duration = Duration::from_millis(ATTEMPT_TIMEOUT_MILLIS as u64);
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Atomic gate linearizing action-dispatch authorization with fatal closure.
pub(crate) struct ActionDispatchGate {
    closed: std::sync::atomic::AtomicBool,
}

impl Default for ActionDispatchGate {
    fn default() -> Self {
        Self {
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

impl ActionDispatchGate {
    pub(crate) fn try_acquire(self: &Arc<Self>) -> Option<ActionDispatchPermit> {
        (!self.closed.load(Ordering::SeqCst)).then(|| ActionDispatchPermit {
            _gate: Arc::clone(self),
        })
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

/// Authorization acquired immediately before invoking an outbound request.
#[must_use = "keep the permit alive until the request has been dispatched"]
pub(crate) struct ActionDispatchPermit {
    _gate: Arc<ActionDispatchGate>,
}

/// Safe-to-report failures in the required watch runtime.
#[derive(Debug, Error)]
pub(crate) enum RuntimeError {
    /// State was unreadable, malformed, unsupported or inconsistent.
    #[error("watch state is unavailable or invalid")]
    InvalidState,
    /// A valid record could not be compared against recovery time.
    #[error("watch recovery requires a comparable host clock")]
    RecoveryTimeUnavailable,
    /// A required atomic state transition or filesystem operation failed.
    #[error("watch state could not be durably updated")]
    Persistence,
    /// A checked clock or state-machine transition failed.
    #[error("watch coordinator transition failed")]
    Transition,
    /// A pending firing cannot be paired with its saved non-secret action identity.
    #[error("pending watch action does not match its saved action identity")]
    ActionSnapshotMismatch,
    /// The shared generic action request could not be constructed safely.
    #[error("watch action request could not be constructed")]
    ActionConstruction,
    /// The coordinator lock was poisoned.
    #[error("watch coordinator state is unavailable")]
    Poisoned,
    /// A prior fatal admission failure closed the coordinator.
    #[error("watch coordinator is already failed")]
    Fatal,
    /// An owned action task exited unexpectedly.
    #[error("watch action task failed")]
    ActionTaskFailed,
}

/// Process-owned coordinator; its advisory file lock is held for its lifetime.
pub(crate) struct WatchService {
    state: Mutex<WatchState>,
    fatal: tokio::sync::watch::Sender<bool>,
    dispatch_gate: Arc<ActionDispatchGate>,
    client: reqwest::Client,
    monotonic_origin: Instant,
}

struct WatchState {
    store: DurableStore,
    configs: BTreeMap<String, RiteWatch>,
}

struct DurableStore {
    path: PathBuf,
    _lock: File,
    file: WatchFile,
    epoch: ClockEpoch,
}

/// One reserved attempt, ready for the shared generic HTTP request builder.
struct ReservedAction {
    watch_name: String,
    firing: FiringRecord,
    action: RiteAction,
}

/// Read-only config view used by pre-bind serving initialization.
pub(crate) struct RiteConfigView<'a> {
    /// Configured watches.
    pub watches: &'a [RiteWatch],
    /// Explicit durable state file path.
    pub state_path: Option<&'a PathBuf>,
}

impl WatchService {
    /// Open, validate, recover and durably commit watch state before listener bind.
    pub(crate) fn open(
        config: &RiteConfigView<'_>,
        dispatch_gate: Arc<ActionDispatchGate>,
    ) -> Result<Option<Arc<Self>>, RuntimeError> {
        if config.watches.is_empty() {
            return Ok(None);
        }
        let path = config.state_path.ok_or(RuntimeError::InvalidState)?;
        Self::open_inner(path, config.watches, dispatch_gate, || {
            let host_utc_ms = system_utc_millis().ok_or(RuntimeError::RecoveryTimeUnavailable)?;
            Ok((host_utc_ms, 0, Instant::now()))
        })
        .map(Some)
    }

    #[cfg(test)]
    fn open_at(
        path: &Path,
        watches: &[RiteWatch],
        host_utc_ms: i64,
        monotonic_anchor_ms: u64,
    ) -> Result<Arc<Self>, RuntimeError> {
        Self::open_inner(
            path,
            watches,
            Arc::new(ActionDispatchGate::default()),
            || Ok((host_utc_ms, monotonic_anchor_ms, Instant::now())),
        )
    }

    fn open_inner(
        path: &Path,
        watches: &[RiteWatch],
        dispatch_gate: Arc<ActionDispatchGate>,
        sample_clock: impl FnOnce() -> Result<(i64, u64, Instant), RuntimeError>,
    ) -> Result<Arc<Self>, RuntimeError> {
        let configs = watches
            .iter()
            .cloned()
            .map(|watch| (watch.name.clone(), watch))
            .collect::<BTreeMap<_, _>>();
        if configs.len() != watches.len() {
            return Err(RuntimeError::InvalidState);
        }
        let lock = open_private(&lock_path(path))?;
        lock.try_lock_exclusive()
            .map_err(|_| RuntimeError::Persistence)?;
        let existing = read_state_file(path)?;
        let (host_utc_ms, monotonic_anchor_ms, monotonic_origin) = sample_clock()?;
        let mut file = match existing {
            Some(file) => file,
            None => WatchFile::empty(host_utc_ms).map_err(|_| RuntimeError::Transition)?,
        };
        let recovery_clock = RecoveryClock {
            host_utc_ms: Some(host_utc_ms),
            monotonic_anchor_ms: Some(monotonic_anchor_ms),
            // SystemTime is the declared environmental host-time
            // assumption; hidden cross-boot manipulation is not detectable here.
            host_time_trusted: true,
        };
        let recovered_before_reconcile =
            recover(&file, recovery_clock).map_err(|error| match error {
                WatchError::HostTimeUnavailable => RuntimeError::RecoveryTimeUnavailable,
                WatchError::InvalidRecord => RuntimeError::InvalidState,
                _ => RuntimeError::Transition,
            })?;
        apply_recovered_records(&mut file, &recovered_before_reconcile)?;
        // Advance only the validated in-memory snapshot before add/remove
        // reconciliation stamps records with the sampled recovery instant.
        file.advance_time(host_utc_ms)
            .map_err(|_| RuntimeError::RecoveryTimeUnavailable)?;
        reconcile_watch_config(&mut file, &configs, host_utc_ms)?;
        // Record validity is checked independently from host-clock comparability.
        file.validate().map_err(|_| RuntimeError::InvalidState)?;
        let recovered = recover(&file, recovery_clock).map_err(|error| match error {
            WatchError::HostTimeUnavailable => RuntimeError::RecoveryTimeUnavailable,
            WatchError::InvalidRecord => RuntimeError::InvalidState,
            _ => RuntimeError::Transition,
        })?;
        apply_recovered_records(&mut file, &recovered)?;
        file.validate().map_err(|_| RuntimeError::InvalidState)?;
        let mut store = DurableStore {
            path: path.to_owned(),
            _lock: lock,
            file,
            epoch: recovered.epoch,
        };
        store.persist()?;
        let (fatal, _) = tokio::sync::watch::channel(false);
        let client = reqwest::Client::builder()
            .timeout(ACTION_TIMEOUT)
            .build()
            .map_err(|_| RuntimeError::ActionConstruction)?;
        Ok(Arc::new(Self {
            state: Mutex::new(WatchState { store, configs }),
            fatal,
            dispatch_gate,
            client,
            monotonic_origin,
        }))
    }

    /// Observe an already authenticated and normalized external event.
    pub(crate) fn observe(&self, event: &RiteEvent) -> Result<(), RuntimeError> {
        self.observe_with_clock(event, || self.monotonic_millis())
    }

    #[cfg(test)]
    fn observe_at(&self, event: &RiteEvent, monotonic_ms: u64) -> Result<(), RuntimeError> {
        self.observe_with_clock(event, || Ok(monotonic_ms))
    }

    fn observe_with_clock(
        &self,
        event: &RiteEvent,
        clock: impl FnOnce() -> Result<u64, RuntimeError>,
    ) -> Result<(), RuntimeError> {
        self.observe_with_lock_hook(event, || {}, clock)
    }

    fn observe_with_lock_hook(
        &self,
        event: &RiteEvent,
        before_lock: impl FnOnce(),
        clock: impl FnOnce() -> Result<u64, RuntimeError>,
    ) -> Result<(), RuntimeError> {
        before_lock();
        let Ok(mut state) = self.state.lock() else {
            self.signal_fatal();
            return Err(RuntimeError::Poisoned);
        };
        if self.is_fatal() {
            return Err(RuntimeError::Fatal);
        }
        let result = (|| {
            let monotonic_ms = clock()?;
            let now = state
                .store
                .epoch
                .utc_at(monotonic_ms)
                .map_err(|_| RuntimeError::Transition)?;
            let mut changed = false;
            let WatchState { store, configs } = &mut *state;
            for (name, config) in configs {
                if !config.matches(event) {
                    continue;
                }
                let record = store
                    .file
                    .watches
                    .get_mut(name)
                    .ok_or(RuntimeError::InvalidState)?;
                record
                    .observe_heartbeat(now, config.window_secs)
                    .map_err(|_| RuntimeError::Transition)?;
                changed = true;
            }
            if changed {
                store
                    .file
                    .advance_time(now)
                    .map_err(|_| RuntimeError::Transition)?;
                store.persist()?;
            }
            Ok(())
        })();
        if result.is_err() {
            self.signal_fatal();
        }
        result
    }

    /// Deterministic coordinator transition; production supplies elapsed monotonic time.
    #[cfg(test)]
    fn step_at(&self, monotonic_ms: u64) -> Result<Option<ReservedAction>, RuntimeError> {
        self.step_with_clock(|| Ok(monotonic_ms))
    }

    fn step_with_clock(
        &self,
        clock: impl FnOnce() -> Result<u64, RuntimeError>,
    ) -> Result<Option<ReservedAction>, RuntimeError> {
        self.step_with_lock_hook(|| {}, clock)
    }

    fn step_with_lock_hook(
        &self,
        before_lock: impl FnOnce(),
        clock: impl FnOnce() -> Result<u64, RuntimeError>,
    ) -> Result<Option<ReservedAction>, RuntimeError> {
        before_lock();
        let Ok(mut state) = self.state.lock() else {
            self.signal_fatal();
            return Err(RuntimeError::Poisoned);
        };
        if self.is_fatal() {
            return Err(RuntimeError::Fatal);
        }
        let result = Self::step_locked(&mut state, clock);
        if result.is_err() {
            self.signal_fatal();
        }
        result
    }

    fn step_locked(
        state: &mut WatchState,
        clock: impl FnOnce() -> Result<u64, RuntimeError>,
    ) -> Result<Option<ReservedAction>, RuntimeError> {
        let monotonic_ms = clock()?;
        let now = state
            .store
            .epoch
            .utc_at(monotonic_ms)
            .map_err(|_| RuntimeError::Transition)?;
        let names = state.configs.keys().cloned().collect::<Vec<_>>();
        let mut changed = false;
        for name in &names {
            let config = state
                .configs
                .get(name)
                .cloned()
                .ok_or(RuntimeError::InvalidState)?;
            let record = state
                .store
                .file
                .watches
                .get_mut(name)
                .ok_or(RuntimeError::InvalidState)?;
            if record
                .exhaust_crashed_final_attempt(now)
                .map_err(|_| RuntimeError::Transition)?
            {
                changed = true;
                continue;
            }
            let fingerprint =
                action_fingerprint(&config.action).map_err(|_| RuntimeError::ActionConstruction)?;
            if record
                .fire_if_due(name, now, config.window_secs, &fingerprint)
                .map_err(|_| RuntimeError::Transition)?
            {
                changed = true;
            }
        }
        let mut reserved = None;
        for name in names {
            let config = state
                .configs
                .get(&name)
                .cloned()
                .ok_or(RuntimeError::InvalidState)?;
            let record = state
                .store
                .file
                .watches
                .get_mut(&name)
                .ok_or(RuntimeError::InvalidState)?;
            if let Some(firing) = record
                .reserve_attempt(now)
                .map_err(|_| RuntimeError::Transition)?
            {
                changed = true;
                reserved = Some(ReservedAction {
                    watch_name: name,
                    firing,
                    action: config.action.clone(),
                });
                break;
            }
        }
        if changed {
            state
                .store
                .file
                .advance_time(now)
                .map_err(|_| RuntimeError::Transition)?;
            state.store.persist()?;
        }
        Ok(reserved)
    }

    /// Run the supervised one-second evaluator; fatal persistence errors return to the CLI.
    pub(crate) async fn run(self: Arc<Self>) -> Result<(), RuntimeError> {
        let mut interval = tokio::time::interval(EVALUATION_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut actions = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.schedule_tick(&mut actions, self.monotonic_millis()?)?;
                }
                result = actions.join_next(), if !actions.is_empty() => {
                    match result {
                        Some(Ok(Err(error))) => return Err(error),
                        Some(Err(_)) => {
                            self.signal_fatal();
                            return Err(RuntimeError::ActionTaskFailed);
                        }
                        Some(Ok(Ok(()))) | None => {}
                    }
                }
            }
        }
    }

    fn schedule_tick(
        self: &Arc<Self>,
        actions: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
        monotonic_ms: u64,
    ) -> Result<(), RuntimeError> {
        if let Some(reserved) = self.step_with_clock(|| Ok(monotonic_ms))? {
            let service = Arc::clone(self);
            actions.spawn(async move {
                let accepted = service.execute(&reserved).await;
                service.finish_with_clock(&reserved, accepted, || service.monotonic_millis())
            });
        }
        Ok(())
    }

    async fn execute(&self, reserved: &ReservedAction) -> bool {
        let RiteAction::HttpPost {
            url,
            headers,
            body_template,
            body_json,
        } = &reserved.action;
        let Ok(request) = http_post_request(
            &self.client,
            url,
            headers,
            body_template.as_deref(),
            body_json.as_ref(),
            &reserved.firing.event,
        ) else {
            return false;
        };
        // Permit acquisition is the dispatch linearization point; fatal closure
        // prevents any subsequently authorized action from starting.
        let Some(_permit) = self.dispatch_gate.try_acquire() else {
            return false;
        };
        match timeout(ACTION_TIMEOUT, request.send()).await {
            Ok(Ok(response)) => response.status().is_success(),
            Ok(Err(_)) | Err(_) => false,
        }
    }

    #[cfg(test)]
    fn finish(
        &self,
        reserved: &ReservedAction,
        accepted: bool,
        monotonic_ms: u64,
    ) -> Result<(), RuntimeError> {
        self.finish_with_clock(reserved, accepted, || Ok(monotonic_ms))
    }

    fn finish_with_clock(
        &self,
        reserved: &ReservedAction,
        accepted: bool,
        clock: impl FnOnce() -> Result<u64, RuntimeError>,
    ) -> Result<(), RuntimeError> {
        self.finish_with_lock_hook(reserved, accepted, || {}, clock)
    }

    fn finish_with_lock_hook(
        &self,
        reserved: &ReservedAction,
        accepted: bool,
        before_lock: impl FnOnce(),
        clock: impl FnOnce() -> Result<u64, RuntimeError>,
    ) -> Result<(), RuntimeError> {
        before_lock();
        let Ok(mut state) = self.state.lock() else {
            self.signal_fatal();
            return Err(RuntimeError::Poisoned);
        };
        if self.is_fatal() {
            return Err(RuntimeError::Fatal);
        }
        let result = Self::finish_locked(&mut state, reserved, accepted, clock);
        if result.is_err() {
            self.signal_fatal();
        }
        result
    }

    fn finish_locked(
        state: &mut WatchState,
        reserved: &ReservedAction,
        accepted: bool,
        clock: impl FnOnce() -> Result<u64, RuntimeError>,
    ) -> Result<(), RuntimeError> {
        let monotonic_ms = clock()?;
        let now = state
            .store
            .epoch
            .utc_at(monotonic_ms)
            .map_err(|_| RuntimeError::Transition)?;
        let record = state
            .store
            .file
            .watches
            .get_mut(&reserved.watch_name)
            .ok_or(RuntimeError::InvalidState)?;
        let current = record.pending.as_ref().ok_or(RuntimeError::InvalidState)?;
        if current.id != reserved.firing.id
            || current.attempts_reserved != reserved.firing.attempts_reserved
        {
            return Err(RuntimeError::InvalidState);
        }
        record
            .complete_attempt(now, accepted)
            .map_err(|_| RuntimeError::Transition)?;
        state
            .store
            .file
            .advance_time(now)
            .map_err(|_| RuntimeError::Transition)?;
        state.store.persist()?;
        Ok(())
    }

    /// Wait for a fatal state-admission failure to be supervised by the CLI.
    pub(crate) async fn fatal(&self) {
        let mut receiver = self.fatal.subscribe();
        if *receiver.borrow() {
            return;
        }
        let _ = receiver.changed().await;
    }

    pub(crate) fn is_fatal(&self) -> bool {
        self.dispatch_gate.is_closed() || *self.fatal.borrow()
    }

    pub(crate) fn signal_fatal(&self) {
        self.dispatch_gate.close();
        self.fatal.send_replace(true);
    }

    fn monotonic_millis(&self) -> Result<u64, RuntimeError> {
        u64::try_from(self.monotonic_origin.elapsed().as_millis())
            .map_err(|_| RuntimeError::Transition)
    }

    #[cfg(test)]
    fn snapshot(&self) -> Result<WatchFile, RuntimeError> {
        self.state
            .lock()
            .map(|state| state.store.file.clone())
            .map_err(|_| RuntimeError::Poisoned)
    }
}

fn read_state_file(path: &Path) -> Result<Option<WatchFile>, RuntimeError> {
    let file = match fs::read(path) {
        Ok(bytes) => {
            let file: WatchFile =
                serde_json::from_slice(&bytes).map_err(|_| RuntimeError::InvalidState)?;
            file.validate().map_err(|_| RuntimeError::InvalidState)?;
            Some(file)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return Err(RuntimeError::InvalidState),
    };
    Ok(file)
}

fn reconcile_watch_config(
    file: &mut WatchFile,
    configs: &BTreeMap<String, RiteWatch>,
    host_utc_ms: i64,
) -> Result<(), RuntimeError> {
    // The fingerprint is a secret-free immutable binding to a pending action snapshot.
    for (name, record) in &file.watches {
        if let (Some(firing), Some(config)) = (&record.pending, configs.get(name)) {
            let configured_fingerprint =
                action_fingerprint(&config.action).map_err(|_| RuntimeError::ActionConstruction)?;
            if configured_fingerprint != firing.action_fingerprint {
                return Err(RuntimeError::ActionSnapshotMismatch);
            }
        }
    }
    for (name, record) in &mut file.watches {
        if !configs.contains_key(name) {
            record
                .remove_with_pending(host_utc_ms)
                .map_err(|_| RuntimeError::Transition)?;
        }
    }
    for (name, watch) in configs {
        if !file.watches.contains_key(name) {
            file.watches.insert(
                name.clone(),
                WatchRecord::new(host_utc_ms, watch.window_secs)
                    .map_err(|_| RuntimeError::Transition)?,
            );
            tracing::info!(watch = %name, "new dead-man watch armed with a durable initial window");
        }
    }
    Ok(())
}

impl DurableStore {
    fn persist(&mut self) -> Result<(), RuntimeError> {
        self.file
            .validate()
            .map_err(|_| RuntimeError::InvalidState)?;
        let payload = serde_json::to_vec(&self.file).map_err(|_| RuntimeError::Persistence)?;
        atomic_replace(&self.path, &payload).map_err(|_| RuntimeError::Persistence)
    }
}

fn atomic_replace(path: &Path, payload: &[u8]) -> std::io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let filename = path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "state path has no filename",
        )
    })?;
    let temp_path = loop {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(
            ".{}.{}.{}.tmp",
            filename.to_string_lossy(),
            std::process::id(),
            sequence
        ));
        match open_private_new(&candidate) {
            Ok(mut file) => {
                if let Err(error) = file.write_all(payload).and_then(|()| file.sync_all()) {
                    drop(file);
                    let _ = fs::remove_file(&candidate);
                    return Err(error);
                }
                drop(file);
                break candidate;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    };
    if let Err(error) = fs::rename(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }
    File::open(parent)?.sync_all()
}

fn open_private(path: &Path) -> Result<File, RuntimeError> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(path)
        .map_err(|_| RuntimeError::Persistence)
}

fn open_private_new(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

fn lock_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".lock");
    PathBuf::from(name)
}

fn system_utc_millis() -> Option<i64> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    i64::try_from(elapsed.as_millis()).ok()
}

fn apply_recovered_records(
    file: &mut WatchFile,
    recovered: &RecoveredFile,
) -> Result<(), RuntimeError> {
    for (name, recovered) in &recovered.watches {
        let record = file
            .watches
            .get_mut(name)
            .ok_or(RuntimeError::InvalidState)?;
        *record = recovered.record.clone();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn heartbeat() -> RiteEvent {
        RiteEvent {
            source: "github".into(),
            event_type: "push".into(),
            action: None,
            timestamp: chrono::DateTime::from_timestamp_millis(1).unwrap(),
            severity: rite_core::Severity::Info,
            title: "source event".into(),
            body: Some("synthetic heartbeat payload".into()),
            metadata: BTreeMap::new(),
        }
    }
    use axum::{Router, http::StatusCode, routing::post};
    use rite_core::watch::{AttemptPhase, TerminalResult};
    use std::{
        collections::BTreeMap,
        fs,
        net::SocketAddr,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use tokio::{sync::oneshot, task::JoinHandle};
    use url::Url;

    fn directory() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "rite-watch-runtime-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("synthetic watch directory");
        path
    }

    fn watch(url: &str) -> RiteWatch {
        RiteWatch {
            name: "watch-a".into(),
            watch_source: vec!["github".into()],
            matcher: BTreeMap::from([(
                "event_type".into(),
                rite_core::MatchValue::String("push".into()),
            )]),
            window_secs: 60,
            action: RiteAction::HttpPost {
                url: Url::parse(url).expect("synthetic URL"),
                headers: BTreeMap::from([("x-runtime-token".into(), "never-persist-this".into())]),
                body_template: None,
                body_json: None,
            },
        }
    }

    #[test]
    fn fatal_signal_is_latched_before_supervisor_subscribes() {
        let dir = directory();
        let service = WatchService::open_at(
            &dir.join("state.json"),
            &[watch("http://localhost/action")],
            0,
            0,
        )
        .unwrap();
        let in_flight = service.dispatch_gate.try_acquire().unwrap();
        service.signal_fatal();
        let mut receiver = service.fatal.subscribe();
        assert!(
            *receiver.borrow_and_update(),
            "a late subscriber must observe an earlier fatal signal"
        );
        assert!(service.dispatch_gate.try_acquire().is_none());
        drop(in_flight);
        drop(service);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn fatal_coordinator_rejects_later_observations_without_persisting() {
        let dir = directory();
        let path = dir.join("state.json");
        let service =
            WatchService::open_at(&path, &[watch("http://localhost/action")], 0, 0).unwrap();
        let bytes_before = fs::read(&path).unwrap();
        service.signal_fatal();

        assert!(matches!(
            service.observe_at(&heartbeat(), 10),
            Err(RuntimeError::Fatal)
        ));
        assert_eq!(fs::read(&path).unwrap(), bytes_before);

        drop(service);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn observation_persistence_failure_latches_fatal_before_unlock() {
        let dir = directory();
        let path = dir.join("state.json");
        let service =
            WatchService::open_at(&path, &[watch("http://localhost/action")], 0, 0).unwrap();
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();

        assert!(matches!(
            service.observe_at(&heartbeat(), 10),
            Err(RuntimeError::Persistence)
        ));
        assert!(service.is_fatal());
        assert!(matches!(
            service.observe_at(&heartbeat(), 20),
            Err(RuntimeError::Fatal)
        ));

        drop(service);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn evaluator_persistence_failure_latches_fatal_before_unlock() {
        let dir = directory();
        let path = dir.join("state.json");
        let service =
            WatchService::open_at(&path, &[watch("http://localhost/action")], 0, 0).unwrap();
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();

        assert!(matches!(
            service.step_at(60_000),
            Err(RuntimeError::Persistence)
        ));
        assert!(service.is_fatal());

        drop(service);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn completion_persistence_failure_latches_fatal_before_unlock() {
        let dir = directory();
        let path = dir.join("state.json");
        let service =
            WatchService::open_at(&path, &[watch("http://localhost/action")], 0, 0).unwrap();
        let reserved = service.step_at(60_000).unwrap().unwrap();
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();

        assert!(matches!(
            service.finish(&reserved, true, 60_001),
            Err(RuntimeError::Persistence)
        ));
        assert!(service.is_fatal());

        drop(service);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn reconciliation_can_add_a_watch_after_durable_time_advances() {
        let dir = directory();
        let path = dir.join("state.json");
        let original = watch("http://127.0.0.1:9/action");
        drop(WatchService::open_at(&path, std::slice::from_ref(&original), 0, 0).unwrap());

        let mut added = watch("http://127.0.0.1:9/action");
        added.name = "watch-b".into();
        let service = WatchService::open_at(&path, &[original, added], 100, 100).unwrap();
        let state = service.snapshot().unwrap();
        assert_eq!(state.high_water_utc_ms, 100);
        assert_eq!(state.watches["watch-b"].arm_started_utc_ms, 100);
        drop(service);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn reconciliation_can_remove_pending_watch_after_durable_time_advances() {
        let dir = directory();
        let path = dir.join("state.json");
        let original = watch("http://127.0.0.1:9/action");
        let mut retained = watch("http://127.0.0.1:9/action");
        retained.name = "watch-b".into();
        retained.window_secs = 600;
        let service = WatchService::open_at(&path, &[original, retained.clone()], 0, 0).unwrap();
        assert_eq!(
            service.step_at(60_000).unwrap().unwrap().watch_name,
            "watch-a"
        );
        drop(service);

        let reopened = WatchService::open_at(&path, &[retained], 61_000, 100).unwrap();
        let state = reopened.snapshot().unwrap();
        assert_eq!(state.high_water_utc_ms, 61_000);
        assert!(state.watches["watch-a"].pending.is_none());
        let outcome = state.watches["watch-a"].last_outcome.as_ref().unwrap();
        assert_eq!(outcome.finished_at_utc_ms, 61_000);
        assert_eq!(outcome.result, TerminalResult::Removed);
        assert!(state.watches["watch-b"].pending.is_none());
        drop(reopened);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn concurrent_heartbeats_sample_clocks_in_durable_lock_order() {
        const WORKERS: usize = 24;
        let dir = directory();
        let path = dir.join("state.json");
        let service =
            WatchService::open_at(&path, &[watch("http://127.0.0.1:9/action")], 0, 0).unwrap();
        let start = Arc::new(std::sync::Barrier::new(WORKERS));
        let clock = Arc::new(AtomicUsize::new(1));
        let workers = (0..WORKERS)
            .map(|_| {
                let service = Arc::clone(&service);
                let start = Arc::clone(&start);
                let clock = Arc::clone(&clock);
                std::thread::spawn(move || {
                    start.wait();
                    service.observe_with_clock(&heartbeat(), || {
                        Ok(u64::try_from(clock.fetch_add(1, Ordering::Relaxed)).unwrap())
                    })
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap().unwrap();
        }
        let state = service.snapshot().unwrap();
        assert_eq!(state.high_water_utc_ms, i64::try_from(WORKERS).unwrap());
        assert_eq!(
            state.watches["watch-a"].last_seen_utc_ms,
            Some(i64::try_from(WORKERS).unwrap())
        );
        drop(service);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn evaluator_samples_monotonic_time_after_acquiring_state_lock() {
        let dir = directory();
        let path = dir.join("state.json");
        let service =
            WatchService::open_at(&path, &[watch("http://127.0.0.1:9/action")], 0, 0).unwrap();
        let guard = service.state.lock().unwrap();
        let sampled = Arc::new(AtomicBool::new(false));
        let (started, waiting) = std::sync::mpsc::channel();
        let runner = Arc::clone(&service);
        let worker_sampled = Arc::clone(&sampled);
        let worker = std::thread::spawn(move || {
            runner.step_with_lock_hook(
                || started.send(()).unwrap(),
                || {
                    worker_sampled.store(true, Ordering::Release);
                    Ok(0)
                },
            )
        });
        waiting.recv().unwrap();
        assert!(!sampled.load(Ordering::Acquire));
        drop(guard);
        worker.join().unwrap().unwrap();
        assert!(sampled.load(Ordering::Acquire));
        drop(service);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn action_completion_samples_monotonic_time_after_acquiring_state_lock() {
        let dir = directory();
        let path = dir.join("state.json");
        let service =
            WatchService::open_at(&path, &[watch("http://127.0.0.1:9/action")], 0, 0).unwrap();
        let reserved = service.step_at(60_000).unwrap().unwrap();
        let guard = service.state.lock().unwrap();
        let sampled = Arc::new(AtomicBool::new(false));
        let (started, waiting) = std::sync::mpsc::channel();
        let runner = Arc::clone(&service);
        let worker_sampled = Arc::clone(&sampled);
        let worker = std::thread::spawn(move || {
            runner.finish_with_lock_hook(
                &reserved,
                true,
                || started.send(()).unwrap(),
                || {
                    worker_sampled.store(true, Ordering::Release);
                    Ok(60_001)
                },
            )
        });
        waiting.recv().unwrap();
        assert!(!sampled.load(Ordering::Acquire));
        drop(guard);
        worker.join().unwrap().unwrap();
        assert!(sampled.load(Ordering::Acquire));
        drop(service);
        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn durable_file_reopen_preserves_initial_deadline_and_crash_reservation() {
        let dir = directory();
        let path = dir.join("state.json");
        let watch = watch("http://127.0.0.1:9/action");
        let first = WatchService::open_at(&path, std::slice::from_ref(&watch), 10_000, 0).unwrap();
        assert_eq!(
            first.snapshot().unwrap().watches["watch-a"].last_seen_utc_ms,
            None
        );
        let deadline = first.snapshot().unwrap().watches["watch-a"].silence_deadline_utc_ms;
        assert_eq!(deadline, 70_000);
        drop(first);
        let middle =
            WatchService::open_at(&path, std::slice::from_ref(&watch), 30_000, 700).unwrap();
        assert_eq!(
            middle.snapshot().unwrap().watches["watch-a"].silence_deadline_utc_ms,
            deadline
        );
        assert!(middle.step_at(40_699).unwrap().is_none());
        let reservation = middle
            .step_at(40_700)
            .unwrap()
            .expect("saved boundary fires");
        assert_eq!(reservation.firing.id, "watch-a:1");
        assert_eq!(reservation.firing.attempts_reserved, 1);
        drop(middle);
        let recovered = WatchService::open_at(&path, &[watch], 70_000, 900).unwrap();
        let pending = recovered.snapshot().unwrap().watches["watch-a"]
            .pending
            .clone()
            .unwrap();
        assert!(pending.uncertain);
        assert_eq!(pending.id, "watch-a:1");
        assert_eq!(pending.attempts_reserved, 1);
        let AttemptPhase::Attempting {
            recovery_at_utc_ms, ..
        } = pending.phase
        else {
            panic!("reservation stays pending")
        };
        assert!(recovered.step_at(15_899).unwrap().is_none());
        let next = recovered
            .step_at(15_900)
            .unwrap()
            .expect("reservation time + timeout + delay");
        assert_eq!(next.firing.id, pending.id);
        assert_eq!(next.firing.attempts_reserved, 2);
        drop(recovered);
        fs::remove_dir_all(dir).unwrap();
        assert_eq!(recovery_at_utc_ms, 85_000);
    }

    #[test]
    fn long_downtime_fires_once_on_first_evaluation_without_catchup() {
        let dir = directory();
        let path = dir.join("state.json");
        let configured = watch("http://127.0.0.1:9/action");
        let first =
            WatchService::open_at(&path, std::slice::from_ref(&configured), 10_000, 0).unwrap();
        drop(first);
        let recovered = WatchService::open_at(&path, &[configured], 1_000_000, 0).unwrap();
        let first_fire = recovered
            .step_at(0)
            .unwrap()
            .expect("long outage is due immediately");
        assert_eq!(first_fire.firing.id, "watch-a:1");
        assert!(
            recovered.step_at(0).unwrap().is_none(),
            "pending firing prevents catch-up"
        );
        assert_eq!(recovered.snapshot().unwrap().watches["watch-a"].sequence, 1);
        drop(recovered);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn heartbeat_during_backoff_reopens_with_pending_identity_and_retry_boundary_unchanged() {
        let dir = directory();
        let path = dir.join("state.json");
        let configured = watch("http://127.0.0.1:9/action");
        let service =
            WatchService::open_at(&path, std::slice::from_ref(&configured), 0, 0).unwrap();
        let attempt = service.step_at(60_000).unwrap().unwrap();
        service.finish(&attempt, false, 60_001).unwrap();
        let id = attempt.firing.id;
        service.observe_at(&heartbeat(), 61_000).unwrap();
        let saved = service.snapshot().unwrap().watches["watch-a"].clone();
        assert_eq!(saved.silence_deadline_utc_ms, 121_000);
        assert_eq!(saved.pending.as_ref().unwrap().id, id);
        assert_eq!(
            saved.pending.as_ref().unwrap().phase,
            AttemptPhase::Waiting {
                outcome_at_utc_ms: 60_001,
                retry_at_utc_ms: 65_001,
            }
        );
        drop(service);
        let recovered = WatchService::open_at(&path, &[configured], 62_000, 100).unwrap();
        let pending = recovered.snapshot().unwrap().watches["watch-a"].clone();
        assert_eq!(pending.silence_deadline_utc_ms, 121_000);
        assert_eq!(pending.pending.as_ref().unwrap().id, id);
        assert!(recovered.step_at(3_100).unwrap().is_none());
        let retry = recovered.step_at(3_101).unwrap().unwrap();
        assert_eq!(retry.firing.id, id);
        assert_eq!(retry.firing.attempts_reserved, 2);
        drop(recovered);
        fs::remove_dir_all(dir).unwrap();
    }

    async fn ack(
        State((counter, state_path, durable_reservations)): State<(
            Arc<AtomicUsize>,
            PathBuf,
            Arc<AtomicUsize>,
        )>,
    ) -> StatusCode {
        let ordinal = counter.fetch_add(1, Ordering::Relaxed) + 1;
        if let Ok(bytes) = fs::read(state_path)
            && let Ok(file) = serde_json::from_slice::<WatchFile>(&bytes)
            && file.watches["watch-a"]
                .pending
                .as_ref()
                .is_some_and(|firing| {
                    usize::from(firing.attempts_reserved) == ordinal
                        && matches!(firing.phase, AttemptPhase::Attempting { .. })
                })
        {
            durable_reservations.fetch_add(1, Ordering::Relaxed);
        }
        StatusCode::NO_CONTENT
    }
    use axum::extract::State;

    async fn receiver(
        counter: Arc<AtomicUsize>,
        state_path: PathBuf,
        durable_reservations: Arc<AtomicUsize>,
    ) -> (String, oneshot::Sender<()>, JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address: SocketAddr = listener.local_addr().unwrap();
        let (stop, signal) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/action", post(ack)).with_state((
                    counter,
                    state_path,
                    durable_reservations,
                )),
            )
            .with_graceful_shutdown(async { _ = signal.await })
            .await
            .unwrap();
        });
        (format!("http://{address}/action"), stop, task)
    }

    async fn hold_connection(
        State(accepted): State<tokio::sync::mpsc::UnboundedSender<()>>,
    ) -> StatusCode {
        accepted.send(()).unwrap();
        std::future::pending().await
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_watch_action_does_not_block_the_next_evaluation_tick() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, mut accepted_rx) = tokio::sync::mpsc::unbounded_channel();
        let app = Router::new()
            .route("/action", post(hold_connection))
            .with_state(accepted_tx);
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = directory();
        let path = dir.join("state.json");
        let url = format!("http://{address}/action");
        let first = watch(&url);
        let mut second = watch(&url);
        second.name = "watch-b".into();
        let mut service = WatchService::open_at(&path, &[first, second], 0, 0).unwrap();
        Arc::get_mut(&mut service).unwrap().monotonic_origin = Instant::now()
            .checked_sub(Duration::from_secs(120))
            .unwrap();
        let runner = Arc::clone(&service);
        let run = tokio::spawn(async move { runner.run().await });

        accepted_rx.recv().await.expect("first action started");
        tokio::time::advance(EVALUATION_INTERVAL).await;
        let next_action = tokio::time::timeout(Duration::from_millis(100), accepted_rx.recv())
            .await
            .is_ok_and(|accepted| accepted.is_some());

        run.abort();
        let _ = run.await;
        server.abort();
        let _ = server.await;
        drop(service);
        fs::remove_dir_all(dir).unwrap();

        assert!(
            next_action,
            "a pending HTTP action must not stall later deadline evaluation"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn real_action_request_is_bounded_by_ten_virtual_seconds() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, mut accepted_rx) = tokio::sync::mpsc::unbounded_channel();
        let app = Router::new()
            .route("/action", post(hold_connection))
            .with_state(accepted_tx);
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = directory();
        let path = dir.join("state.json");
        let configured = watch(&format!("http://{address}/action"));
        let service = WatchService::open_at(&path, &[configured], 0, 0).unwrap();
        let reserved = service.step_at(60_000).unwrap().unwrap();
        let runner = Arc::clone(&service);
        let request = tokio::spawn(async move { runner.execute(&reserved).await });
        accepted_rx.recv().await.unwrap();
        tokio::time::advance(ACTION_TIMEOUT).await;
        assert!(!request.await.unwrap());
        server.abort();
        let _ = server.await;
        drop(service);
        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn real_receiver_duplicate_tolerance_and_recorded_acceptance_survive_reopen() {
        let dir = directory();
        let path = dir.join("state.json");
        let counter = Arc::new(AtomicUsize::new(0));
        let durable_reservations = Arc::new(AtomicUsize::new(0));
        let (url, stop, task) =
            receiver(counter.clone(), path.clone(), durable_reservations.clone()).await;
        let mut configured = watch(&url);
        configured.window_secs = 4;
        let service =
            WatchService::open_at(&path, std::slice::from_ref(&configured), 50_000, 0).unwrap();
        let reserved = service.step_at(4_000).unwrap().expect("due initial arm");
        assert_eq!(reserved.firing.event.source, "monitor");
        assert_eq!(reserved.firing.event.event_type, "deadline_exceeded");
        assert!(reserved.firing.event.body.is_none());
        assert!(service.execute(&reserved).await);
        let firing_id = reserved.firing.id.clone();
        // Model the remote receiver accepting the request while the local
        // outcome record is lost: the same stable firing is retried.
        service.finish(&reserved, false, 4_001).unwrap();
        let retry = service
            .step_at(9_001)
            .unwrap()
            .expect("persisted five-second wait");
        assert_eq!(retry.firing.id, firing_id);
        assert!(service.execute(&retry).await);
        service.finish(&retry, true, 9_002).unwrap();
        assert_eq!(durable_reservations.load(Ordering::Relaxed), 2);
        service.observe_at(&heartbeat(), 9_003).unwrap();
        assert_eq!(counter.load(Ordering::Relaxed), 2);
        let outcome = service.snapshot().unwrap().watches["watch-a"]
            .last_outcome
            .as_ref()
            .unwrap()
            .clone();
        assert_eq!(outcome.result, TerminalResult::Accepted);
        assert!(outcome.uncertain);
        assert_eq!(outcome.attempts_reserved, 2);
        let saved = fs::read_to_string(&path).unwrap();
        assert!(!saved.contains("never-persist-this"));
        assert!(!saved.contains("heartbeat payload"));
        drop(service);
        let reopened = WatchService::open_at(&path, &[configured], 60_000, 100).unwrap();
        assert!(
            reopened.snapshot().unwrap().watches["watch-a"]
                .pending
                .is_none()
        );
        assert_eq!(
            reopened.snapshot().unwrap().watches["watch-a"]
                .last_outcome
                .as_ref()
                .unwrap()
                .result,
            TerminalResult::Accepted
        );
        assert!(reopened.step_at(100).unwrap().is_none());
        let _ = stop.send(());
        task.await.unwrap();
        drop(reopened);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn observed_retry_boundaries_exhaust_three_reservations_without_a_fourth() {
        let dir = directory();
        let path = dir.join("state.json");
        let configured = watch("http://127.0.0.1:9/action");
        let service =
            WatchService::open_at(&path, std::slice::from_ref(&configured), 0, 0).unwrap();
        let first = service.step_at(60_000).unwrap().unwrap();
        service.finish(&first, false, 60_001).unwrap();
        drop(service);
        let recovered =
            WatchService::open_at(&path, std::slice::from_ref(&configured), 60_001, 0).unwrap();
        assert!(recovered.step_at(4_999).unwrap().is_none());
        let second = recovered.step_at(5_000).unwrap().unwrap();
        assert_eq!(second.firing.attempts_reserved, 2);
        recovered.finish(&second, false, 5_001).unwrap();
        drop(recovered);
        let final_boot = WatchService::open_at(&path, &[configured], 65_002, 0).unwrap();
        assert!(final_boot.step_at(29_999).unwrap().is_none());
        let third = final_boot.step_at(30_000).unwrap().unwrap();
        assert_eq!(third.firing.attempts_reserved, 3);
        final_boot.finish(&third, false, 30_001).unwrap();
        let outcome = final_boot.snapshot().unwrap().watches["watch-a"]
            .last_outcome
            .clone()
            .unwrap();
        assert_eq!(outcome.result, TerminalResult::RetryExhausted);
        assert_eq!(outcome.attempts_reserved, 3);
        assert!(outcome.uncertain);
        assert!(
            final_boot.snapshot().unwrap().watches["watch-a"]
                .pending
                .is_none()
        );
        drop(final_boot);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn durable_file_reopen_preserves_every_crash_ordinal_and_never_reserves_a_fourth() {
        for ordinal in 1_u8..=3 {
            let dir = directory();
            let path = dir.join("state.json");
            let configured = watch("http://127.0.0.1:9/action");
            let service =
                WatchService::open_at(&path, std::slice::from_ref(&configured), 0, 0).unwrap();
            let first = service.step_at(60_000).unwrap().unwrap();
            let mut pending = first.firing.clone();
            if ordinal >= 2 {
                service.finish(&first, false, 60_001).unwrap();
                let second = service.step_at(65_001).unwrap().unwrap();
                pending = second.firing.clone();
                if ordinal == 3 {
                    service.finish(&second, false, 65_002).unwrap();
                    pending = service.step_at(95_002).unwrap().unwrap().firing;
                }
            }
            assert_eq!(pending.attempts_reserved, ordinal);
            let AttemptPhase::Attempting {
                reserved_at_utc_ms: reserved_at,
                ..
            } = pending.phase
            else {
                panic!("attempt ordinal {ordinal} was not durably reserved");
            };
            let original_silence_deadline =
                service.snapshot().unwrap().watches["watch-a"].silence_deadline_utc_ms;
            drop(service);

            let first_recovery = WatchService::open_at(
                &path,
                std::slice::from_ref(&configured),
                reserved_at + 1_000,
                100,
            )
            .unwrap();
            assert_eq!(
                first_recovery.snapshot().unwrap().watches["watch-a"].silence_deadline_utc_ms,
                original_silence_deadline
            );
            assert_eq!(
                first_recovery.snapshot().unwrap().watches["watch-a"]
                    .pending
                    .as_ref()
                    .unwrap()
                    .attempts_reserved,
                ordinal
            );
            drop(first_recovery);

            let recovery_delay = match ordinal {
                1 => 15_000,
                2 => 40_000,
                3 => 10_000,
                _ => unreachable!(),
            };
            let recovery_at = reserved_at + recovery_delay;
            let second_recovery =
                WatchService::open_at(&path, &[configured], reserved_at + 2_000, 200).unwrap();
            let boundary_mono = 200 + u64::try_from(recovery_at - reserved_at - 2_000).unwrap();
            assert!(
                second_recovery
                    .step_at(boundary_mono - 1)
                    .unwrap()
                    .is_none()
            );
            let due = second_recovery.step_at(boundary_mono).unwrap();
            if ordinal == 3 {
                assert!(due.is_none());
                let record = second_recovery.snapshot().unwrap().watches["watch-a"].clone();
                assert!(record.pending.is_none());
                let terminal = record.last_outcome.unwrap();
                assert_eq!(terminal.result, TerminalResult::RetryExhausted);
                assert_eq!(terminal.attempts_reserved, 3);
                assert!(terminal.uncertain);
            } else {
                assert_eq!(due.unwrap().firing.attempts_reserved, ordinal + 1);
            }
            drop(second_recovery);
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn valid_record_plus_host_clock_failure_preserves_the_exact_durable_bytes() {
        let dir = directory();
        let path = dir.join("state.json");
        let configured = watch("http://127.0.0.1:9/action");
        let service =
            WatchService::open_at(&path, std::slice::from_ref(&configured), 10_000, 0).unwrap();
        drop(service);
        let before = fs::read(&path).unwrap();
        let decoded: WatchFile = serde_json::from_slice(&before).unwrap();
        decoded.validate().unwrap();
        assert_eq!(
            recover(
                &decoded,
                RecoveryClock {
                    host_utc_ms: Some(9_999),
                    monotonic_anchor_ms: Some(0),
                    host_time_trusted: true
                }
            ),
            Err(WatchError::HostTimeUnavailable)
        );
        assert_eq!(fs::read(&path).unwrap(), before);
        assert!(WatchService::open_at(&path, &[configured], 9_999, 0).is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
        fs::remove_dir_all(dir).unwrap();
    }
}
