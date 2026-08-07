# Iron Consensus

Iron Consensus is a planned multi-service distributed transaction and replicated-state-machine system in Rust. The repository currently contains **Layer 1 only**: validated shared types and configuration, a protocol-neutral envelope and bounded deduplication table, a durable single-file WAL, transport and pure protocol boundaries, and deterministic test support.

Two-Phase Commit, Three-Phase Commit, Raft, Multi-Paxos, gRPC services, deployable binaries, and a production runtime are deliberately not implemented yet. Their milestones and the invariants Layer 1 establishes are recorded in [the project plan](docs/PROJECT_PLAN.md).

## Build and test

The workspace pins Rust 1.85.0. With that toolchain and `cargo-deny` installed, run:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo deny check
```

No service can be started in Layer 1 because it intentionally contains libraries and cross-crate tests only. The next milestone adds the Tokio runtime, Protobuf/gRPC adapter, and independently deployable process shells.

## Workspace

- `iron-core`: validated identities, limits/configuration, envelopes, fingerprints, and deterministic bounded deduplication.
- `iron-wal`: locked `wal-v1.log` append, durability barriers, strict replay, corruption detection, and policy-controlled torn-tail repair.
- `iron-transport`: framework-neutral at-least-once asynchronous send and inbound-delivery contracts.
- `iron-protocol`: deterministic protocol event, recovery, transition, and post-durability action vocabulary.
- `iron-testkit`: logical clock, deterministic scheduler/network faults, seeded scenario helpers, and WAL fixtures.
- `iron-foundation-tests`: integration tests across the five foundation libraries.

Layer 1 is verified on Rust 1.85.0. Formatting, strict Clippy, all 54 unit and integration tests, and the dependency policy audit pass locally. `Cargo.lock` is committed with Cargo's MSRV-aware resolver so transitive dependencies remain compatible with the pinned toolchain.
