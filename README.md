# Iron Consensus

Iron Consensus is a multi-service distributed transaction and replicated-state-machine system in Rust. The repository contains the Layer 1 foundation plus deterministic protocol cores, a Protobuf/gRPC adapter, WAL-before-effects runtime composition, and deployable listener shells.

The protocol cores for Two-Phase Commit, Three-Phase Commit, Raft, and Multi-Paxos now exist as deterministic, I/O-free state-machine crates, with deployable coordinator, participant, and consensus-node gRPC binaries. Outbound routing, snapshots, membership changes, and production hardening remain staged work in [the project plan](docs/PROJECT_PLAN.md).

## Build and test

The workspace pins Rust 1.85.0. With that toolchain, `protoc`, and `cargo-deny` installed, run:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo deny check
```

The runtime, Protobuf envelope adapter, and independently deployable gRPC listener shells are present. Handlers now dispatch validated commands into the 2PC and Raft state machines; outbound action routing, durable protocol records, and lifecycle instrumentation remain staged work.

## Workspace

- `iron-core`: validated identities, limits/configuration, envelopes, fingerprints, and deterministic bounded deduplication.
- `iron-wal`: locked `wal-v1.log` append, durability barriers, strict replay, corruption detection, and policy-controlled torn-tail repair.
- `iron-transport`: framework-neutral at-least-once asynchronous send and inbound-delivery contracts.
- `iron-protocol`: deterministic protocol event, recovery, transition, and post-durability action vocabulary.
- `iron-testkit`: logical clock, deterministic scheduler/network faults, seeded scenario helpers, and WAL fixtures.
- `iron-foundation-tests`: integration tests across the five foundation libraries.
- `iron-runtime`: WAL-before-effects protocol execution and recovery composition.
- `iron-grpc`: validated Protobuf v1 envelope conversion.
- `iron-2pc`, `iron-3pc`: deterministic transaction state machines.
- `iron-raft`, `iron-multipaxos`: deterministic consensus cores.
- `iron-coordinator`, `iron-participant`, `iron-consensus-node`: independently deployable gRPC listener shells (ports 7001–7003 by default; override with `IRON_LISTEN_ADDR`).

The workspace is verified on Rust 1.85.0. Formatting, strict Clippy, all 60 unit and integration tests, and the dependency policy audit pass locally. `Cargo.lock` is committed with Cargo's MSRV-aware resolver so transitive dependencies remain compatible with the pinned toolchain.
