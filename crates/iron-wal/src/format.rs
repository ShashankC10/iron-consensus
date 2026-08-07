use iron_core::{Lsn, SchemaVersion};

use crate::{CorruptionKind, UnsupportedField, WalError, WalRecordInput};

/// Fixed v1 file name within the configured WAL directory.
pub const WAL_FILE_NAME: &str = "wal-v1.log";
/// Four-byte v1 frame marker.
pub const MAGIC: [u8; 4] = *b"ICWL";
/// Supported binary format version.
pub const FORMAT_VERSION: u16 = 1;
/// Exact v1 header length.
pub const HEADER_LEN: usize = 40;

const HEADER_LEN_U16: u16 = 40;
const HEADER_LEN_U32: u32 = 40;
const FLAGS_V1: u32 = 0;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DecodedHeader {
    pub total_frame_len: u32,
    pub lsn_raw: u64,
    pub record_kind: u16,
    pub schema_version_raw: u16,
    pub payload_len: u32,
    pub payload_crc32c: u32,
}

pub(crate) fn encode_frame(lsn: Lsn, record: WalRecordInput<'_>) -> Result<Vec<u8>, WalError> {
    let payload_len =
        u32::try_from(record.payload().len()).map_err(|_| WalError::RecordTooLarge {
            actual: u64::try_from(record.payload().len()).unwrap_or(u64::MAX),
            maximum: u32::MAX - HEADER_LEN_U32,
        })?;
    let total_frame_len =
        HEADER_LEN_U32
            .checked_add(payload_len)
            .ok_or(WalError::RecordTooLarge {
                actual: u64::from(payload_len),
                maximum: u32::MAX - HEADER_LEN_U32,
            })?;

    let mut frame = vec![0_u8; HEADER_LEN + record.payload().len()];
    frame[0..4].copy_from_slice(&MAGIC);
    frame[4..6].copy_from_slice(&FORMAT_VERSION.to_be_bytes());
    frame[6..8].copy_from_slice(&HEADER_LEN_U16.to_be_bytes());
    frame[8..12].copy_from_slice(&total_frame_len.to_be_bytes());
    frame[12..16].copy_from_slice(&FLAGS_V1.to_be_bytes());
    frame[16..24].copy_from_slice(&lsn.get().to_be_bytes());
    frame[24..26].copy_from_slice(&record.record_kind().to_be_bytes());
    frame[26..28].copy_from_slice(&record.schema_version().get().to_be_bytes());
    frame[28..32].copy_from_slice(&payload_len.to_be_bytes());
    frame[32..36].copy_from_slice(&crc32c::crc32c(record.payload()).to_be_bytes());
    let header_crc = crc32c::crc32c(&frame[0..36]);
    frame[36..40].copy_from_slice(&header_crc.to_be_bytes());
    frame[HEADER_LEN..].copy_from_slice(record.payload());
    Ok(frame)
}

pub(crate) fn decode_header(
    header: &[u8; HEADER_LEN],
    offset: u64,
    last_valid_lsn: Option<Lsn>,
) -> Result<DecodedHeader, WalError> {
    if header[0..4] != MAGIC {
        return Err(corruption(offset, last_valid_lsn, CorruptionKind::BadMagic));
    }

    let expected_header_crc = read_u32(header, 36);
    let actual_header_crc = crc32c::crc32c(&header[0..36]);
    if expected_header_crc != actual_header_crc {
        return Err(corruption(
            offset,
            last_valid_lsn,
            CorruptionKind::HeaderChecksum,
        ));
    }

    let version = read_u16(header, 4);
    if version != FORMAT_VERSION {
        return Err(WalError::UnsupportedFormat {
            offset,
            field: UnsupportedField::FormatVersion,
            actual: u64::from(version),
        });
    }
    let header_len = read_u16(header, 6);
    if header_len != HEADER_LEN_U16 {
        return Err(WalError::UnsupportedFormat {
            offset,
            field: UnsupportedField::HeaderLength,
            actual: u64::from(header_len),
        });
    }
    let flags = read_u32(header, 12);
    if flags != FLAGS_V1 {
        return Err(WalError::UnsupportedFormat {
            offset,
            field: UnsupportedField::Flags,
            actual: u64::from(flags),
        });
    }

    let total_frame_len = read_u32(header, 8);
    let payload_len = read_u32(header, 28);
    let expected_frame_len = HEADER_LEN_U32
        .checked_add(payload_len)
        .ok_or_else(|| corruption(offset, last_valid_lsn, CorruptionKind::LengthMismatch))?;
    if total_frame_len != expected_frame_len {
        return Err(corruption(
            offset,
            last_valid_lsn,
            CorruptionKind::LengthMismatch,
        ));
    }

    let record_kind = read_u16(header, 24);
    let schema_version_raw = read_u16(header, 26);
    if record_kind == 0 || schema_version_raw == 0 {
        return Err(corruption(
            offset,
            last_valid_lsn,
            CorruptionKind::ZeroRecordMetadata,
        ));
    }

    Ok(DecodedHeader {
        total_frame_len,
        lsn_raw: read_u64(header, 16),
        record_kind,
        schema_version_raw,
        payload_len,
        payload_crc32c: read_u32(header, 32),
    })
}

pub(crate) fn validate_payload_checksum(
    payload: &[u8],
    expected: u32,
    offset: u64,
    last_valid_lsn: Option<Lsn>,
) -> Result<(), WalError> {
    if crc32c::crc32c(payload) != expected {
        return Err(corruption(
            offset,
            last_valid_lsn,
            CorruptionKind::PayloadChecksum,
        ));
    }
    Ok(())
}

pub(crate) fn validate_lsn(
    actual: u64,
    last_valid_lsn: Option<Lsn>,
    offset: u64,
) -> Result<Lsn, WalError> {
    let expected = match last_valid_lsn {
        Some(previous) => previous.checked_successor().ok_or(WalError::LsnExhausted)?,
        None => Lsn::FIRST,
    };
    if actual != expected.get() {
        return Err(corruption(
            offset,
            last_valid_lsn,
            CorruptionKind::LsnSequence {
                expected: expected.get(),
                actual,
            },
        ));
    }
    Ok(expected)
}

pub(crate) fn decode_schema_version(
    raw: u16,
    offset: u64,
    last_valid_lsn: Option<Lsn>,
) -> Result<SchemaVersion, WalError> {
    SchemaVersion::try_from(raw)
        .map_err(|_| corruption(offset, last_valid_lsn, CorruptionKind::ZeroRecordMetadata))
}

fn read_u16(bytes: &[u8; HEADER_LEN], offset: usize) -> u16 {
    u16::from_be_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u32(bytes: &[u8; HEADER_LEN], offset: usize) -> u32 {
    u32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn read_u64(bytes: &[u8; HEADER_LEN], offset: usize) -> u64 {
    u64::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

fn corruption(offset: u64, last_valid_lsn: Option<Lsn>, kind: CorruptionKind) -> WalError {
    WalError::Corruption {
        offset,
        last_valid_lsn,
        kind,
    }
}

#[cfg(test)]
mod tests {
    use iron_core::{Lsn, SchemaVersion};

    use super::{HEADER_LEN, encode_frame};
    use crate::WalRecordInput;

    #[test]
    fn frame_has_exact_header_and_payload_lengths() {
        let schema = SchemaVersion::try_from(1).expect("one is nonzero");
        let input = WalRecordInput::new(1_024, schema, b"abc").expect("kind is nonzero");
        let frame = encode_frame(Lsn::FIRST, input).expect("small frame encodes");
        let golden: [u8; HEADER_LEN + 3] = [
            73, 67, 87, 76, 0, 1, 0, 40, 0, 0, 0, 43, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 4, 0, 0,
            1, 0, 0, 0, 3, 54, 75, 63, 183, 124, 119, 10, 152, 97, 98, 99,
        ];
        assert_eq!(frame, golden);
    }
}
