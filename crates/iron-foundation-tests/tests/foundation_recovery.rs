use iron_core::{
    BeginResult, ClusterId, DEFAULT_MAX_RECORD_BYTES, DedupConfig, DedupKey, DedupTable, MessageId,
    NodeId, RawSyncPolicy, RestoreResult, SemanticFingerprint, TailRepair,
};
use iron_testkit::WalFixture;
use iron_wal::{FileWal, WalRecordInput};

fn opaque(last_byte: u8) -> [u8; 16] {
    let mut bytes = [0_u8; 16];
    bytes[15] = last_byte;
    bytes
}

#[test]
fn append_flush_crash_replay_restores_completed_dedup_outcome() {
    let fixture = WalFixture::new().expect("isolated WAL fixture is created");
    let config = fixture
        .config(
            u64::from(DEFAULT_MAX_RECORD_BYTES),
            RawSyncPolicy::Manual,
            TailRepair::Reject,
        )
        .expect("fixture configuration is valid");

    let key = DedupKey::new(
        ClusterId::from_bytes(opaque(1)).expect("cluster ID is nonzero"),
        NodeId::parse("node-a").expect("node ID is valid"),
        MessageId::from_bytes(opaque(2)).expect("message ID is nonzero"),
    );
    let fingerprint = SemanticFingerprint::from_bytes([7_u8; 32]);
    let outcome = b"committed".to_vec();

    let mut volatile = DedupTable::new(DedupConfig::default());
    assert_eq!(
        volatile
            .begin(&key, fingerprint)
            .expect("dedup counter has capacity"),
        BeginResult::New
    );
    volatile
        .complete(&key, fingerprint, outcome.clone())
        .expect("outcome respects configured bounds");

    let mut wal = FileWal::open(config.clone()).expect("WAL opens");
    let record = WalRecordInput::new(
        1_024,
        1_u16.try_into().expect("schema version is nonzero"),
        &outcome,
    )
    .expect("protocol record kind is nonzero");
    let append = wal.append(record).expect("record is fully written");
    assert_eq!(append.durable_through(), None);
    assert_eq!(
        wal.flush().expect("explicit durability barrier"),
        Some(append.lsn())
    );

    drop(volatile);
    drop(wal);

    let mut restarted_wal = FileWal::open(config).expect("restart scans and locks WAL");
    let replay = restarted_wal.replay().expect("durable frame replays");
    assert_eq!(replay.records().len(), 1);
    assert_eq!(replay.records()[0].lsn(), append.lsn());

    let mut restored = DedupTable::new(DedupConfig::default());
    for record in replay.records() {
        assert_eq!(
            restored
                .restore_completed(&key, fingerprint, record.payload().to_vec())
                .expect("recovery uses deterministic dedup algorithm"),
            RestoreResult::Restored
        );
    }
    match restored
        .begin(&key, fingerprint)
        .expect("restored lookup does not allocate")
    {
        BeginResult::Replay(replayed) => assert_eq!(replayed, outcome),
        other => panic!("expected exact retained replay, got {other:?}"),
    }
}

#[test]
fn explicit_tail_fixture_is_reported_and_repaired_only_by_policy() {
    let fixture = WalFixture::new().expect("isolated WAL fixture is created");
    let truncate_config = fixture
        .config(
            u64::from(DEFAULT_MAX_RECORD_BYTES),
            RawSyncPolicy::Always,
            TailRepair::Truncate,
        )
        .expect("fixture configuration is valid");
    {
        let mut wal = FileWal::open(truncate_config.clone()).expect("WAL opens");
        let record = WalRecordInput::new(
            1_024,
            1_u16.try_into().expect("schema version is nonzero"),
            b"valid",
        )
        .expect("record kind is nonzero");
        wal.append(record).expect("valid frame appends");
    }
    let valid_length = fixture.wal_len().expect("fixture length is readable");
    fixture
        .append_raw(b"torn")
        .expect("explicit torn bytes are appended");

    let wal = FileWal::open(truncate_config).expect("truncate policy repairs tail");
    let repair = wal
        .open_report()
        .tail_repair()
        .expect("repair details are reported");
    assert_eq!(repair.offset(), valid_length);
    assert_eq!(repair.bytes_removed(), 4);
    assert_eq!(
        fixture.wal_len().expect("repaired length is readable"),
        valid_length
    );
}
