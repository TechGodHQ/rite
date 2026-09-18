# Inbox-to-notification delivery contract (v1)

## Status

This document materializes the COD-498 round-3 agreement as a design and
synthetic-fixture gate. It does not implement a new source, action, route,
configuration value, subscription checkpoint, retry loop, deployment, or
notification. The current Iris SSE source and generic `http_post` action keep
their existing behavior until their separately scoped tickets land.

The goal is one bounded producer-consumer seam: selected agent reply,
attention, and execution-error occurrences become durable Iris records and may
then be routed by Rite to a static, user-chosen destination. The seam is
explicitly **not** a generic remote-control channel for an agent.

## Ownership boundary

| Responsibility | Owner | Constraint |
| --- | --- | --- |
| Source normalization, source installation identity, authenticated ingest, durable inbox history, and committed-event publication | Iris | Source adapters/mappers remain in Iris provider libraries; public operations are Hydra-projected. |
| Local hook receipt and durable pre-ingest spool | Iris `contrib/claude-hooks` bridge (COD-495–497) | Receives local hooks only; never controls sessions, grants permissions, or reads/scrapes transcripts. |
| Selection, static destination policy, durable subscription receipt/checkpoint, per-handler planning/outcomes, and operator recovery | Rite | Sources/actions stay generic; no agent-specific core branch or provider-named action type. |
| Destination acceptance/read semantics | Destination | Rite may observe only its named receiving boundary; it cannot claim a user read receipt. |

The bridge does not move into Rite. Rite does not install hooks or normalize
agent-specific payloads. Metadata is a selector, never an authorization
boundary; COD-493 owns source authorization and credential policy.

## V1 envelope and semantic paths

The committed synthetic matrix lives in
[`tests/fixtures/inbox-notifications/v1/fixtures.json`](../../tests/fixtures/inbox-notifications/v1/fixtures.json).
Every positive case is a pair of one normalized Iris `Message` and the Rite
`RiteEvent` that the existing Iris source produces from it.

The bridge sets `Message.kind = "text"`; semantic notification kind is never
inferred from `kind`, `Stop`, sender role, or body text. It is carried under
these exact Rite metadata paths:

| Meaning | Literal Rite path |
| --- | --- |
| schema version | `["metadata", "message", "metadata", "schema_version"]` |
| semantic event kind | `["metadata", "message", "metadata", "event_kind"]` |
| source installation | `["metadata", "message", "metadata", "installation_id"]` |
| session | `["metadata", "message", "metadata", "session_id"]` |
| stable occurrence | `["metadata", "message", "metadata", "occurrence_id"]` |
| hook kind | `["metadata", "message", "metadata", "hook_kind"]` |
| explicit direction | `["metadata", "message", "metadata", "direction"]` |
| explicit content consent | `["metadata", "message", "metadata", "content_opt_in"]` |
| Iris message identity | `["metadata", "message", "id"]` |
| Iris thread identity | `["metadata", "message", "thread_id"]` |
| original source/provenance | `["metadata", "message", "source"]` and `metadata.iris_metadata` compatibility copy |

`event_kind` is exactly one of `turn_ended`, `attention_required`, or
`execution_error` in v1. Optional `turn_id` and `prompt_id` appear only when
supplied by the source. The occurrence ID and receipt timestamp are fixed at
first bridge receipt and remain unchanged across retries.

Iris COD-491 owns collision-isolated mapping of
`(source, installation_id, session_id, occurrence_id)` to normalized model
IDs. The fixture mirrors that contract: two installations can reuse a session
ID; two equal text bodies with distinct occurrence IDs are distinct messages.

### Direction and loop suppression

`Message.is_outbound` means direction relative to the configured Iris owner.
Bridge-received agent occurrences are inbound (`false`) even when an assistant
wrote their text. The bridge may set `true` only from an explicit
source-authoritative owner-originated fact. The matching `metadata.direction`
value is `inbound` or `owner_originated`; no consumer derives it from sender,
body, destination, or a provider name.

Loop suppression requires all of these static configured checks before Rite
selects an event:

1. source allowlist;
2. installation allowlist;
3. `schema_version = 1`;
4. semantic-event-kind allowlist; and
5. the agreed direction/provenance rule; and
6. `content_opt_in = true` before a text-bearing notification is selected or
   planned.

Missing or malformed provenance fails closed for this notification mode. A
`Stop` only means `turn_ended`; it is never evidence that a task succeeded.
Unsupported `StopFailure` must not be fabricated as `execution_error`.

## Content and destination policy

Only explicitly opted-in reply/alert text may populate the Iris message body.
The bridge does not include a transcript, tool payload, raw working directory,
credential, destination URL, or permission prompt by default. Fixtures use
synthetic text only.

Destinations, target identifiers, credential references, and handler
configuration are static local configuration. They must never be interpolated
from a message. No message-derived URL, header, recipient, token, or command
is permitted. Replies to a delivered notification do not control Claude, a
hook bridge, Iris, or Rite in v1.

The existing generic `http_post` action remains the only contemplated action
shape. COD-500 owns JSON-safe structured action payloads; it must preserve the
existing text-template behavior and cannot introduce a destination-named action
variant.

## Durable state and delivery guarantees

The following state labels are deliberately distinct:

| Label | Meaning | Does not mean |
| --- | --- | --- |
| **Bridge accepted** | Local bridge has durably enqueued the occurrence for ingest. | Iris committed it, Rite received it, or a destination accepted it. |
| **Iris committed** | Iris durably committed the normalized message. | Rite receipt or notification delivery. |
| **Rite received** | Rite atomically persisted subscription identity, stable Iris message ID, and opaque replay checkpoint. | Handler action was planned, sent, accepted, or read. |
| **Handler planned** | One named handler/version has durable pending work. | Any sibling handler or destination received it. |
| **Destination accepted** | The named remote receiving boundary gave a positive acknowledgement. | A human read or acted on it. |
| **Uncertain** | Dispatch might have crossed the destination boundary but a timeout, connection loss, or crash prevents a safe conclusion. | Safe retry. |

COD-501 stores the receipt/checkpoint and a durable delivery-plan outbox
atomically. Its deduplication key is exactly `(subscription_identity,
iris_message_id)`, not a cursor, timestamp, or body. Before a checkpoint can
advance, the same recoverable transaction (or an equivalently durable outbox
invariant) MUST contain the receipt/dedup evidence, the pinned
handler-configuration version, and pending work for every selected handler. A
receipt/planning storage failure or full pending-work capacity MUST leave the
checkpoint unadvanced and the message available for reconciliation; it MUST NOT
turn a missing plan into a successful receipt. Dedup evidence is retained for
the declared replay/reconciliation horizon; pruning cannot silently make old
sends eligible again.

COD-502 refines that already-durable plan into independent per-handler
outcomes, retry safety, and recovery; it must not weaken the receipt-to-work
invariant or retroactively replan a checkpointed message against changed
configuration. It keeps secrets out of every durable snapshot and log.
Successful sibling handlers never rerun on ordinary replay. A timeout after
dispatch, connection loss after an unknown send, or crash after send before
durable result is **uncertain**. There is no automatic retry unless
non-dispatch is demonstrated or the configured receiving boundary has an
approved idempotency contract. An operator may requeue only through an explicit
action that acknowledges the risk of duplicate delivery.

Static policy selection and durable receipt deduplication are separate stages.
An exact replay that still satisfies the configured provenance, event-kind, and
content-consent checks remains **selected** by the future static selector. The
COD-501 receipt transaction then recognizes the previously persisted
`(subscription_identity, iris_message_id)` and records the explicit
`deduplicated_no_second_receipt` disposition. A selector alone does not claim
to recognize or suppress a replay.

## Recovery and degraded subscriptions

Public replay is a prerequisite, not a result of the private Iris broker or
COD-485 negotiation. An invalid/expired replay cursor or server restart moves
a durable subscription to visible **degraded** state. Already-durable work is
preserved; no hidden future-only reset occurs.

V1 does not promise lossless history catch-up. The operator reconciles or
resets a degraded subscription through an approved generated status/recovery
operation, leaving an explicit unresolved-gap record. A durable backfill can
only use the approved Iris reader/poll contract from COD-491/COD-492; it must
not scrape private snapshots, repurpose COD-463 timestamp polling, or treat
COD-475 outbound history as the inbox reader.

Finite capacity/retention values, credential/reference storage policy, and
operator authorization are intentional design/security gates. They are not
filled in by a runtime default, fixture, or runner assumption.

## Dependency-ordered delivery slices

1. **COD-491** — finalize durable inbox identity, reader, ordering, and recovery
   contract; **COD-492** adds generated reads; **COD-493** adds source-scoped
   authorization; **COD-494** publishes committed events.
2. **COD-495–497** — map local hook events to the agreed envelope, spool safely,
   package/install it, and run a public-boundary acceptance path.
3. **COD-499** — add explicit nested predicates so handlers can select the
   literal metadata paths above without changing scalar/compound semantics.
4. **COD-500** — add JSON-safe generic action bodies while preserving existing
   generic HTTP behavior.
5. **COD-501** — atomically add durable subscription receipt/checkpoint state
   and the configuration-pinned pending-work outbox; storage failure or full
   capacity cannot advance the checkpoint.
6. **COD-502** — add per-handler outcomes, uncertainty, safe retry, and
   recovery on the already-durable receipt-to-work invariant.
7. **COD-503** — exercise the fully composed cross-service fixture path and
   publish setup/recovery documentation.

No slice may claim end-to-end exactly-once delivery, live deployment, or human
read behavior without the corresponding durable contract and executable
verification.

## Fixture acceptance matrix

The v1 fixture set provides complete paired Iris-message/Rite-event records
for `turn_ended`, `attention_required`, and `execution_error`; an exact replay
of a stable message identity; same-body distinct occurrences; two installations
sharing a session; quoted/multiline opted-in text; and a mirrored
owner-originated event. Complete negative inputs cover missing installation,
unsupported `stop_failure`, and missing content opt-in. They remain parseable
at the source boundary but are marked to fail policy selection.

Tests parse every positive, replay, and negative Iris message through the real
`IrisSource::parse_message` boundary and assert the literal metadata paths
above, including the consent path, and compare every declared Rite event field.
The replay fixture distinguishes its static selection result from its separate
stateful receipt disposition. Future selection tests must ensure negative cases
never reach a configured handler. They must not substitute a mocked nested JSON
shape for the source adapter.
