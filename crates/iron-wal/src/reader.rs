use std::io::SeekFrom;

use bytes::Bytes;
use iron_core::{Lsn, TailRepair};

use crate::WalError;
use crate::format::{
    HEADER_LEN, decode_header, decode_schema_version, validate_lsn, validate_payload_checksum,
};
use crate::record::{Replay, ReplayReport, TailRepairReport, WalRecord};
use crate::writer::WalIo;

pub(crate) fn scan(
    file: &mut dyn WalIo,
    max_record_bytes: u32,
    tail_policy: TailRepair,
) -> Result<Replay, WalError> {
    let file_length = file
        .file_len()
        .map_err(|source| WalError::io("reading file length", source))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| WalError::io("seeking to WAL start", source))?;

    let mut offset = 0_u64;
    let mut last_valid_lsn = None;
    let mut records = Vec::new();
    let mut record_count = 0_u64;

    while offset < file_length {
        let remaining = file_length - offset;
        if remaining < HEADER_LEN as u64 {
            return finish_torn_tail(
                file,
                records,
                record_count,
                last_valid_lsn,
                offset,
                file_length,
                tail_policy,
            );
        }

        let mut header = [0_u8; HEADER_LEN];
        file.read_exact(&mut header)
            .map_err(|source| WalError::io("reading WAL header", source))?;
        let decoded = decode_header(&header, offset, last_valid_lsn)?;
        if decoded.payload_len > max_record_bytes {
            return Err(WalError::OnDiskRecordTooLarge {
                offset,
                actual: decoded.payload_len,
                maximum: max_record_bytes,
            });
        }

        let frame_end = offset
            .checked_add(u64::from(decoded.total_frame_len))
            .ok_or(WalError::Corruption {
                offset,
                last_valid_lsn,
                kind: crate::CorruptionKind::LengthMismatch,
            })?;
        if frame_end > file_length {
            return finish_torn_tail(
                file,
                records,
                record_count,
                last_valid_lsn,
                offset,
                file_length,
                tail_policy,
            );
        }

        let payload_len =
            usize::try_from(decoded.payload_len).map_err(|_| WalError::OnDiskRecordTooLarge {
                offset,
                actual: decoded.payload_len,
                maximum: max_record_bytes,
            })?;
        let mut payload = vec![0_u8; payload_len];
        file.read_exact(&mut payload)
            .map_err(|source| WalError::io("reading WAL payload", source))?;
        validate_payload_checksum(&payload, decoded.payload_crc32c, offset, last_valid_lsn)?;
        let lsn = validate_lsn(decoded.lsn_raw, last_valid_lsn, offset)?;
        let schema_version =
            decode_schema_version(decoded.schema_version_raw, offset, last_valid_lsn)?;
        records.push(WalRecord::from_validated(
            lsn,
            decoded.record_kind,
            schema_version,
            Bytes::from(payload),
        ));
        record_count = record_count.checked_add(1).ok_or(WalError::LsnExhausted)?;
        last_valid_lsn = Some(lsn);
        offset = frame_end;
    }

    Ok(Replay::new(
        records,
        ReplayReport::new(record_count, last_valid_lsn, file_length, None),
    ))
}

#[allow(clippy::too_many_arguments)]
fn finish_torn_tail(
    file: &mut dyn WalIo,
    records: Vec<WalRecord>,
    record_count: u64,
    last_valid_lsn: Option<Lsn>,
    offset: u64,
    file_length: u64,
    tail_policy: TailRepair,
) -> Result<Replay, WalError> {
    match tail_policy {
        TailRepair::Reject => Err(WalError::TornTail {
            offset,
            file_length,
            last_valid_lsn,
        }),
        TailRepair::Truncate => {
            file.set_len(offset)
                .map_err(|source| WalError::io("truncating torn WAL tail", source))?;
            file.sync_data()
                .map_err(|source| WalError::io("synchronizing repaired WAL tail", source))?;
            let repair = TailRepairReport::new(offset, file_length - offset, last_valid_lsn);
            Ok(Replay::new(
                records,
                ReplayReport::new(record_count, last_valid_lsn, offset, Some(repair)),
            ))
        }
    }
}
