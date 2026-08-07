# Iron Consensus Project Plan

## 1. Outcome and delivery rule

Iron Consensus will be a Rust workspace implementing 2PC, 3PC, Raft, and Multi-Paxos, using Tokio for asynchronous orchestration, gRPC for process boundaries, and a durable WAL for recovery.
It will also provide a deterministic fault simulator and property/integration tests; coordinator and participant roles must be independently deployable processes.

Layer 1 is the next implementation increment for Luna: a compiling, protocol-neutral foundation, without a consensus algorithm or server binary.
The foundation must make invalid identity/configuration state unrepresentable, define durability and delivery semantics, and support deterministic tests.
No later layer may bypass its validated types, envelope, WAL, or protocol/transport boundaries.

## 2. Layer 1 workspace

Use one virtual Cargo workspace with resolver `2`, edition `2024`, and `rust-version = 1.85`; keep production crates under `crates/`.
Layer 1 has no root application package and no `apps/` members.
Commit `Cargo.lock` because the eventual deliverables are applications, not a published library-only workspace.

```text
Cargo.toml
Cargo.lock
rust-toolchain.toml
deny.toml
crates/
  iron-core/
    Cargo.toml
    src/
      lib.rs
      config.rs
      dedup.rs
      envelope.rs
      error.rs
      id.rs
      limits.rs
  iron-wal/
    Cargo.toml
    src/
      lib.rs
      error.rs
      format.rs
      reader.rs
      record.rs
      writer.rs
  iron-transport/
    Cargo.toml
    src/
      lib.rs
      error.rs
      inbound.rs
      transport.rs
  iron-protocol/
    Cargo.toml
    src/
      lib.rs
      action.rs
      error.rs
      event.rs
      protocol.rs
      transition.rs
  iron-testkit/
    Cargo.toml
    src/
      lib.rs
      clock.rs
      driver.rs
      fault.rs
      network.rs
      wal_fixture.rs
  iron-foundation-tests/
    Cargo.toml
    src/lib.rs
    tests/{foundation_recovery.rs,foundation_simulation.rs}
```

The virtual root lists exactly six members: the five foundation libraries and `crates/iron-foundation-tests`.
The test package is non-publishable, depends on all five libraries, and contains only cross-crate integration tests.
Each crate denies unsafe code; the workspace denies warnings in CI, not in developer profiles.

## 3. Dependency direction and ownership

The allowed production dependency graph is acyclic:

```text
iron-core <- iron-wal
iron-core <- iron-transport
iron-core <- iron-protocol
iron-core, iron-wal, iron-transport, iron-protocol <- iron-testkit
five foundation libraries <- iron-foundation-tests
```

`iron-core` owns validated scalar types, configuration, envelopes, limits, and deduplication rules; it performs no filesystem, socket, wall-clock, random-number, or Tokio work.
`iron-wal` owns binary framing, append, sync, scanning, replay, locking, and tail repair; it stores opaque record payloads and never interprets a protocol state machine.
`iron-transport` owns asynchronous at-least-once send and inbound-delivery interfaces; it neither retries nor decides whether a message is a duplicate.
`iron-protocol` owns the pure state-machine boundary and its event/transition/effect vocabulary; it cannot call a WAL, network, clock, random source, or executor directly.
`iron-testkit` owns deterministic clocks, event scheduling, scripted faults, and WAL fixtures.
Production crates must not depend on `iron-testkit`.

The future runtime is the only component allowed to connect protocol transitions to WAL and transport operations.
It will serialize events per protocol instance, durably append a transition, establish a durability barrier, and only then run external effects.

## 4. Validated identities and configuration

Define distinct newtypes for `ClusterId`, `MessageId`, `CorrelationId`, `ClientRequestId`, `TimerId`, `Lsn`, and `NodeId`; the five opaque IDs use 16-byte values and reject all-zero values.
They serialize as lowercase hyphenated UUID strings in human-readable formats and use 16 bytes on binary/wire boundaries.
No core constructor reads system time or randomness; callers receive an `IdGenerator` interface so tests can supply a counter-based generator.
Production UUIDv7 generation is added with the runtime, outside protocol logic.
`Lsn` is a nonzero `u64`, begins at one, supports checked successor only, and has no public unchecked constructor.
`NodeId` is 1–64 bytes of lowercase ASCII letters, digits, or `-`; it must start and end with an alphanumeric character.

Define `ProtocolName` and `MessageType` as separate validated string types: both are 1–64 bytes, lowercase ASCII, and permit letters, digits, `-`, `_`, and `.` only.
Define `SchemaVersion` as a nonzero `u16` and `EnvelopeVersion` as a nonzero `u16`.
Every validated newtype exposes `parse`, `Display`, `FromStr`, and fallible Serde deserialization.

Deserialize external configuration into `RawNodeConfig`; create `NodeConfig` only through `TryFrom<RawNodeConfig>`.
`NodeConfig` contains `cluster_id`, `node_id`, `members`, `wal`, `transport`, and `dedup`.
`members` is a `BTreeMap<NodeId, PeerEndpoint>`, contains the local node exactly once, and cannot contain duplicate endpoints; peer URLs permit only `http` or `https`, and reject credentials, queries, and fragments.
Listen addresses are explicit socket addresses; DNS names are permitted only in advertised peer URLs.

`WalConfig` contains a directory, `max_record_bytes`, `sync_policy`, and `tail_repair`.
The default maximum WAL payload is 8 MiB; accepted values are 1 KiB through 64 MiB.
`TransportConfig` contains the listen address, advertised URL, request timeout, connect timeout, and `max_message_bytes`.
The default message maximum is 4 MiB; it may not exceed the WAL record maximum after framing overhead.
Timeouts are integer milliseconds in the range 1–300,000; zero never means infinite.
`DedupConfig` contains `max_entries`, `max_outcome_bytes`, and `max_total_outcome_bytes`, defaulting to 65,536, 64 KiB, and 64 MiB; the total-byte limit is 64 KiB–1 GiB and at least `max_outcome_bytes`, all values are nonzero, and one outcome cannot exceed `max_message_bytes`.

Validation returns `ValidationErrors`, a nonempty list of `{field_path, code, message}` sorted by field path and code.
Do not fail on the first configuration violation.
Errors must not include secrets or entire payloads.
Provide crate-local non-exhaustive `CoreError`, `WalError`, `TransportError`, and `ProtocolError` enums using `thiserror`.
Preserve typed sources for I/O and codec failures; never classify errors by matching display strings.

## 5. Versioned protocol-neutral envelope

Layer 1 defines `Envelope::V1` with these immutable semantic fields:

- envelope version, fixed to `1` for the first release;
- cluster ID, source node ID, and destination node ID;
- message ID and optional correlation ID;
- protocol name, message type, and protocol payload schema version;
- delivery attempt, a `u32` starting at zero;
- opaque payload bytes bounded by validated transport configuration.

Responses receive a fresh message ID and set correlation ID to the request message ID.
Retries preserve every semantic field and payload, preserve the message ID, and increment only delivery attempt with checked arithmetic.
There is no timestamp, map-valued metadata, architecture-sized integer, or floating-point value in the envelope.
Unknown envelope versions are rejected as `UnsupportedEnvelopeVersion`, never guessed or silently downgraded.
The transport wire adapter added in Layer 2 will use Protobuf, must convert through the validated core constructor, and must never expose generated types to protocol implementations.

Define a stable semantic fingerprint as SHA-256 over a documented, length-prefixed, big-endian encoding of all immutable fields and payload.
Exclude envelope version and delivery attempt only; include IDs, routing, protocol, type, schema version, and payload.
The fingerprint format itself is version `1` and has golden test vectors.
It is not the transport serialization and must not depend on Serde, Protobuf ordering, or Rust enum discriminants.

## 6. Deterministic bounded idempotency

The deduplication key is `(cluster_id, source_node_id, message_id)`; `DedupTable` is single-owner state intended for the future serialized node runtime, not an internally concurrent cache.
It stores a key, semantic fingerprint, state, insertion sequence, and opaque completed outcome bytes.
States are `InFlight` and `Completed`; no state depends on wall-clock time.

Processing is exactly:

1. `begin(key, fingerprint)` returns `New`, `InFlight`, `Replay(outcome)`, `Conflict`, or `Full`.
2. A missing key reserves an `InFlight` entry before state-machine evaluation and returns `New`.
3. The same key and fingerprint returns `InFlight` while reserved or `Replay` when completed.
4. The same key with another fingerprint returns `Conflict` and never replaces the retained entry.
5. `complete` changes the matching reservation to `Completed`; `abort` removes only a matching reservation.
6. Completing an entry records opaque outcome bytes only if they satisfy both configured byte limits.

The table is bounded by entry count and the sum of `Completed` outcome bytes; before `begin` admits a new key, it evicts oldest completed entries until the entry-count bound fits, returning `Full` if only in-flight entries prevent admission.
Before `complete` stores an outcome, it evicts oldest other completed entries until `retained_bytes + new_outcome_bytes <= max_total_outcome_bytes`.
It never evicts `InFlight` entries; configuration guarantees an individually valid outcome fits after completed eviction, and duplicate access or completion does not refresh insertion order.
All ordering uses a checked local sequence counter, not hash-map iteration; use `BTreeMap` plus `VecDeque`.

The guarantee is bounded idempotency, not global exactly-once execution.
A retained completed key replays the exact stored outcome; an evicted key is new work.
The future runtime must place the dedup completion and protocol state change in one WAL transition record before external effects.
Recovery replays completed keys in LSN order through the same admission/eviction algorithm.
An incomplete reservation has no external effect and is absent after restart because effects occur only after durability.
Clients and algorithms must tolerate retries outside the retained window.

## 7. WAL contract and binary format

Layer 1 implements a single file named `wal-v1.log`; rotation, snapshots, and compaction are deferred, and opening takes an exclusive advisory lock that fails with `AlreadyOpen` if another writer owns it.
The implementation uses ordinary read/write/seek operations and exposes a synchronous API.
The future Tokio runtime will invoke blocking WAL work through a dedicated blocking boundary.

Every frame is contiguous and uses this 40-byte, big-endian header:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | magic bytes `ICWL` |
| 4 | 2 | format version, exactly `1` |
| 6 | 2 | header length, exactly `40` |
| 8 | 4 | total frame length |
| 12 | 4 | flags, zero in version 1 |
| 16 | 8 | monotonic LSN |
| 24 | 2 | protocol-neutral record kind |
| 26 | 2 | payload schema version |
| 28 | 4 | payload length |
| 32 | 4 | CRC32C of payload |
| 36 | 4 | CRC32C of header bytes 0–35 |

`total frame length` must equal `40 + payload length`, without padding.
Record kind zero and schema version zero are invalid; kinds 1–1023 are reserved for foundation/runtime records.
Unknown nonzero kinds remain opaque to the WAL and are rejected or interpreted by the replay consumer.
Unknown flags, format versions, or header lengths are hard `UnsupportedFormat` errors.
CRC32C is for corruption detection, not authentication.

The first record has LSN 1 and every later frame has exactly the checked successor of the preceding LSN; the writer derives it only from a successful scan and callers cannot choose it.
Append accepts `{record_kind, schema_version, payload}`, validates limits, encodes one frame, and uses `write_all`.
On any partial/uncertain write or sync error, the writer becomes poisoned and refuses further writes until reopened and scanned.
Append returns `{lsn, durable_through}`; no caller may infer durability merely from a successful write.

`SyncPolicy` has exactly three variants:

- `Always`: call `sync_data` after every frame and return that LSN as durable;
- `Batch { max_unsynced_records }`: sync on the deterministic record-count boundary;
- `Manual`: append without implicit sync and require an explicit `flush` durability barrier.

`Always` is the production default.
Time-based batching is outside the WAL because it would make core behavior nondeterministic.
`flush` calls `sync_data` and advances `durable_through` to the last fully written LSN.
Creation of the directory/file is followed by file and parent-directory synchronization before reporting success.

Open/replay scans strictly from offset zero and never searches ahead for the next magic value; a trailing fragment shorter than 40 bytes is a torn tail.
A valid header whose declared frame extends beyond EOF is also a torn tail.
With `TailRepair::Truncate`, either case truncates to the last valid frame, syncs the file, and reports bytes removed.
With `TailRepair::Reject`, either case returns `TornTail` without mutation.
A bad magic, header CRC, payload CRC, length relationship, flags value, or LSN sequence is corruption even on the final frame.
Such corruption is never auto-repaired; it returns the failing offset and last valid LSN.
This deliberately prefers operator-visible failure over discarding a complete but corrupted frame.

Replay yields borrowed or reference-counted opaque payload bytes in ascending LSN order and a `ReplayReport`.
The report includes record count, last LSN, durable file length, and optional tail-repair details.
An empty new file is valid and has no last LSN.
Replay must enforce configured size limits before allocating payload storage.

## 8. Transport and protocol interfaces

`iron-transport` defines async `Transport::send(envelope)` and `InboundHandler::deliver(envelope)` operations; send success means only that the remote delivery boundary accepted the envelope.
The delivery model is at least once: loss, duplication, delay, reordering, and retry are expected.
Retry policy, backoff, correlation, deduplication, and protocol responses belong to the future runtime.
Transport errors distinguish invalid input, unavailable, timeout, backpressure, remote rejection, and internal failure.
Interfaces accept and return validated core types only.

`iron-protocol` defines a pure `Protocol` state-machine interface with `recover(records)` and `handle(event)` operations; events are `Message`, `ClientRequest`, and `TimerFired` with validated identity and opaque command data.
A successful handler returns one `Transition` containing at most one opaque durable record plus ordered post-durability actions.
Actions are `Send`, `ScheduleTimer`, `CancelTimer`, and `ReplyToClient`.
Timers use logical durations and validated timer IDs; protocols never observe wall-clock timestamps.
The runtime must reject a transition that requests external actions without a durable record when the action depends on a state change.
Recovery consumes records only and emits no external actions; retransmission decisions occur through an explicit post-recovery event.

Protocol methods are deterministic functions of prior state and event.
They may not spawn tasks, block, perform I/O, read environment variables, use ambient randomness, or access system time.
Panics are bugs, not error handling; malformed peer input returns typed rejection errors.

## 9. Deterministic test strategy

`iron-testkit` uses an integer logical tick and an event queue ordered by `(tick, insertion_sequence)`; no simulator test uses sleep, Tokio time, wall-clock deadlines, thread scheduling, or hash-map iteration order.
The default scenario API is fully scripted; randomized property scenarios use `ChaCha8Rng` with an explicit printed seed.
Every failing randomized test must display the seed and minimized action trace.

The simulated network supports scripted drop, duplicate count, delay ticks, reorder-at-tick, directed partition, heal, crash, and restart; faults match message ordinal plus optional source, destination, protocol, and message type predicates.
When multiple faults match, declaration order is the tie-breaker and each rule states whether it is consuming.
Crash discards volatile runtime/dedup state; restart rebuilds only from WAL replay.
WAL byte truncation, bit flip, and sync-failure fixtures are explicit test operations, not transport faults.

Required Layer 1 unit/property coverage includes:

- accepted/rejected boundaries for every ID, name, duration, size, and configuration rule;
- deterministic ordering of aggregated validation errors;
- envelope retry preservation and golden semantic fingerprint vectors;
- dedup replay, conflict, in-flight protection, FIFO eviction, byte limits, and counter overflow;
- WAL frame golden bytes, record-size limits, sequential LSNs, and reopen continuation;
- empty WAL, valid replay, every possible torn-tail cut point, and both tail policies;
- header/payload corruption, invalid lengths/flags/versions, LSN gaps, and writer poisoning;
- sync-policy durable-through behavior using an injectable file/sync seam;
- transport contract tests shared by simulated and future gRPC adapters;
- protocol determinism: identical state/event input yields identical transition bytes and actions;
- simulation reproducibility from the same seed and divergence only when the seed changes.

Cross-crate integration tests cover append/flush/crash/replay/dedup restoration and scripted duplicate delivery.
Tests must assert invariants and externally visible outcomes, not private struct layout.

## 10. Dependency decisions

Centralize dependency versions and feature flags in `[workspace.dependencies]`; use `serde 1`, `thiserror 2`, `bytes 1`, `uuid 1` with Serde only, `sha2 0.10`, and `crc32c 0.6` in foundation crates.
Use `async-trait 0.1` for object-safe transport/protocol boundaries and `tokio 1` with minimal `sync`, `time`, `rt`, and `macros` features where needed.
Use `fs2 0.4` for the exclusive WAL lock and `url 2` for endpoint validation.
Use `proptest 1`, `tempfile 3`, `rand 0.8`, and `rand_chacha 0.3` only in tests or `iron-testkit`.
Avoid `anyhow` in library APIs, global singletons, default hash maps in deterministic state, and framework-specific types in core crates.
Tonic, Prost, tracing, CLI parsing, and metrics dependencies wait until the runtime/gRPC milestone.

## 11. Cross-layer invariants

1. A protocol state change that can affect an external observer is WAL-durable before its action runs.
2. WAL LSNs are gapless, strictly increasing, and assigned only by the WAL writer.
3. Replay of the same valid WAL produces byte-identical reconstructed protocol and retained dedup state.
4. A message ID is immutable across retries; a different fingerprint under the same dedup key is rejected.
5. Bounded dedup behavior is deterministic and honest about eviction; the system never claims global exactly once.
6. Protocol code is pure and cannot directly observe timing, network, filesystem, randomness, or task scheduling.
7. Transport is at least once and carries only validated, versioned envelopes.
8. Unknown versions fail closed, while known envelopes may carry protocol payloads opaque to lower layers.
9. A torn incomplete tail may be truncated by explicit policy; complete corruption is never silently repaired.
10. Simulator ordering and fault application are stable for a given input trace and seed.
11. Cluster/node identity is checked at every inbound boundary before protocol dispatch.
12. No participant, coordinator, or peer acknowledges a durable state change before its configured durability barrier.

## 12. Layer 1 non-goals

Layer 1 does not implement 2PC, 3PC, Raft, Multi-Paxos, elections, quorum tracking, membership changes, or client semantics.
It does not provide gRPC generated code, listeners, TLS, authentication, authorization, observability exporters, or deployment manifests.
It does not provide coordinator/participant binaries, a general runtime, automatic retry loops, snapshotting, log compaction, or WAL segments.
It does not promise Byzantine fault tolerance, exactly-once delivery, distributed transactions across protocols, or compatibility with unknown future versions.
It does not optimize throughput before recovery semantics and deterministic tests are proven.

## 13. Future milestones

Milestone 2 adds `iron-runtime`, `iron-grpc`, Protobuf v1, tracing/metrics, and graceful lifecycle management.
It also adds independently deployable `iron-coordinator`, `iron-participant`, and symmetric `iron-consensus-node` applications.
Milestone 3 adds 2PC with durable coordinator decisions, participant prepare state, retry/recovery, and crash matrix tests.
Milestone 4 adds 3PC with explicit timing assumptions, pre-commit state, partitions, and documented non-blocking limits.
Milestone 5 adds Raft leader election, replicated log, snapshot/install-snapshot, membership changes, and linearizability tests.
Milestone 6 adds Multi-Paxos proposer/acceptor/learner roles, stable ballots, recovery, and refinement/model tests.
Milestone 7 adds WAL segmentation/compaction, TLS and identity, backpressure, rolling-version compatibility, packaging, and deployment hardening.

Protocol crates will be `iron-2pc`, `iron-3pc`, `iron-raft`, and `iron-multipaxos`.
They depend only on `iron-core` and `iron-protocol`; adapters/runtime applications compose them from outside.
The coordinator and participant applications remain separate Cargo packages and process images even when sharing runtime libraries.

## 14. Layer 1 acceptance checklist

- [ ] The workspace tree and dependency direction match this plan with no extra production crate.
- [ ] All six workspace packages, including `crates/iron-foundation-tests`, compile on the pinned toolchain.
- [ ] Public APIs contain no unvalidated external IDs/configuration and no framework-generated types.
- [ ] Envelope versioning, retry rules, fingerprint format, and golden vectors are documented and tested.
- [ ] Dedup behavior is bounded, deterministic, conflict-safe, recoverable from completion records, and exhaustively tested.
- [ ] WAL v1 emits the exact 40-byte header, validates both CRCs, and enforces gapless LSNs.
- [ ] Torn-tail repair and hard-corruption cases behave exactly as specified at every tested cut point.
- [ ] Append/sync failures poison the writer, and durability results never overstate persisted LSNs.
- [ ] Protocol and transport interfaces preserve the WAL-before-effects and at-least-once boundaries.
- [ ] Simulation and property failures are reproducible from a seed and action trace.
- [ ] There are no sleeps, ambient RNG calls, wall-clock reads, or unordered-map decisions in deterministic tests.
- [ ] Rustdoc explains all public error cases, durability guarantees, and recovery behavior.
- [ ] `cargo fmt --all -- --check` passes.
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings` passes.
- [ ] `cargo test --workspace --all-features` passes, including property and integration suites.
- [ ] `cargo deny check` passes after licenses/advisories are configured.

Layer 1 was subsequently verified locally with Rust 1.85.0: formatting, strict Clippy, all 54 unit and integration tests, and the dependency policy audit pass. `Cargo.lock` was generated with MSRV-aware dependency resolution.
Before merge, perform a static architecture review for dependency cycles, public invalid-state escape hatches, nondeterministic collections, and effects that can run before durability.
