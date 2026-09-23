# Inbox notification fixtures v1

These synthetic fixtures are the producer-consumer contract agreed in COD-498.
They contain no real message, transcript, endpoint, token, user identifier, or
working-directory data.

Each positive case pairs an Iris `Message` with the selected public fields of
the `RiteEvent` shape produced by `rite-sources::iris::IrisSource::parse_message`.
The adapter's retained `metadata.message` must equal the paired Iris message;
the fixture omits that mechanically duplicated subtree. Tests must parse the
Iris message through that real adapter, then compare semantic values at the
literal paths in `docs/design/inbox-notifications.md`.

`expected_selection` is policy guidance for the future selector: it is not a
claim that the current flat matcher can traverse the paths yet. Negative cases
must fail selection before any handler action is planned.

`expected_selection` describes only static policy eligibility. An exact replay
can remain statically eligible while its `receipt_context` separately records
the stateful COD-501 result: the already-persisted
`(subscription_identity, iris_message_id)` must produce
`deduplicated_no_second_receipt`. The selector itself does not claim to dedupe
replays.
