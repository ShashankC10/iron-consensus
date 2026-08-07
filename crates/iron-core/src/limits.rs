use std::fmt;
use std::str::FromStr;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::CoreError;

pub const KIBIBYTE: u32 = 1_024;
pub const MEBIBYTE: u32 = 1_024 * KIBIBYTE;
pub const GIBIBYTE: u32 = 1_024 * MEBIBYTE;

pub const WAL_FRAME_OVERHEAD_BYTES: u32 = 40;

pub const MIN_RECORD_BYTES: u32 = KIBIBYTE;
pub const MAX_RECORD_BYTES: u32 = 64 * MEBIBYTE;
pub const DEFAULT_MAX_RECORD_BYTES: u32 = 8 * MEBIBYTE;

pub const MIN_MESSAGE_BYTES: u32 = 1;
pub const MAX_MESSAGE_BYTES: u32 = 64 * MEBIBYTE;
pub const DEFAULT_MAX_MESSAGE_BYTES: u32 = 4 * MEBIBYTE;

pub const MIN_TIMEOUT_MILLIS: u32 = 1;
pub const MAX_TIMEOUT_MILLIS: u32 = 300_000;

pub const DEFAULT_MAX_DEDUP_ENTRIES: u32 = 65_536;
pub const DEFAULT_MAX_OUTCOME_BYTES: u32 = 64 * KIBIBYTE;
pub const MIN_TOTAL_OUTCOME_BYTES: u32 = 64 * KIBIBYTE;
pub const MAX_TOTAL_OUTCOME_BYTES: u32 = GIBIBYTE;
pub const DEFAULT_MAX_TOTAL_OUTCOME_BYTES: u32 = 64 * MEBIBYTE;

macro_rules! bounded_u32 {
    ($name:ident, $kind:literal, $minimum:expr, $maximum:expr) => {
        #[doc = concat!("A validated ", $kind, ".")]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(u32);

        impl $name {
            pub fn new(value: u64) -> Result<Self, CoreError> {
                if !($minimum as u64..=$maximum as u64).contains(&value) {
                    return Err(CoreError::ValueOutOfRange {
                        kind: $kind,
                        actual: value,
                        minimum: $minimum as u64,
                        maximum: $maximum as u64,
                    });
                }
                let value = u32::try_from(value).map_err(|_| CoreError::ValueOutOfRange {
                    kind: $kind,
                    actual: value,
                    minimum: $minimum as u64,
                    maximum: $maximum as u64,
                })?;
                Ok(Self(value))
            }

            pub fn parse(input: &str) -> Result<Self, CoreError> {
                let value = input
                    .parse::<u64>()
                    .map_err(|source| CoreError::InvalidInteger {
                        kind: $kind,
                        source,
                    })?;
                Self::new(value)
            }

            #[must_use]
            pub const fn get(self) -> u32 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, formatter)
            }
        }

        impl FromStr for $name {
            type Err = CoreError;

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                Self::parse(input)
            }
        }

        impl TryFrom<u64> for $name {
            type Error = CoreError;

            fn try_from(value: u64) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for u32 {
            fn from(value: $name) -> Self {
                value.get()
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_u32(self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = u64::deserialize(deserializer)?;
                Self::new(value).map_err(D::Error::custom)
            }
        }
    };
}

bounded_u32!(
    MaxRecordBytes,
    "maximum WAL record bytes",
    MIN_RECORD_BYTES,
    MAX_RECORD_BYTES
);
bounded_u32!(
    MaxMessageBytes,
    "maximum message bytes",
    MIN_MESSAGE_BYTES,
    MAX_MESSAGE_BYTES
);
bounded_u32!(
    TimeoutMillis,
    "timeout milliseconds",
    MIN_TIMEOUT_MILLIS,
    MAX_TIMEOUT_MILLIS
);
bounded_u32!(MaxDedupEntries, "maximum dedup entries", 1, u32::MAX);
bounded_u32!(
    MaxOutcomeBytes,
    "maximum dedup outcome bytes",
    1,
    MAX_TOTAL_OUTCOME_BYTES
);
bounded_u32!(
    MaxTotalOutcomeBytes,
    "maximum total dedup outcome bytes",
    MIN_TOTAL_OUTCOME_BYTES,
    MAX_TOTAL_OUTCOME_BYTES
);
bounded_u32!(
    MaxUnsyncedRecords,
    "maximum unsynced WAL records",
    1,
    u32::MAX
);

impl Default for MaxRecordBytes {
    fn default() -> Self {
        Self(DEFAULT_MAX_RECORD_BYTES)
    }
}

impl Default for MaxMessageBytes {
    fn default() -> Self {
        Self(DEFAULT_MAX_MESSAGE_BYTES)
    }
}

impl Default for MaxDedupEntries {
    fn default() -> Self {
        Self(DEFAULT_MAX_DEDUP_ENTRIES)
    }
}

impl Default for MaxOutcomeBytes {
    fn default() -> Self {
        Self(DEFAULT_MAX_OUTCOME_BYTES)
    }
}

impl Default for MaxTotalOutcomeBytes {
    fn default() -> Self {
        Self(DEFAULT_MAX_TOTAL_OUTCOME_BYTES)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_RECORD_BYTES, MAX_TIMEOUT_MILLIS, MAX_TOTAL_OUTCOME_BYTES, MIN_RECORD_BYTES,
        MIN_TOTAL_OUTCOME_BYTES, MaxDedupEntries, MaxMessageBytes, MaxOutcomeBytes, MaxRecordBytes,
        MaxTotalOutcomeBytes, MaxUnsyncedRecords, TimeoutMillis,
    };

    #[test]
    fn record_size_boundaries_are_exact() {
        assert!(MaxRecordBytes::new(u64::from(MIN_RECORD_BYTES)).is_ok());
        assert!(MaxRecordBytes::new(u64::from(MAX_RECORD_BYTES)).is_ok());
        assert!(MaxRecordBytes::new(u64::from(MIN_RECORD_BYTES - 1)).is_err());
        assert!(MaxRecordBytes::new(u64::from(MAX_RECORD_BYTES) + 1).is_err());
    }

    #[test]
    fn timeout_boundaries_are_exact_and_zero_is_never_infinite() {
        assert!(TimeoutMillis::new(1).is_ok());
        assert!(TimeoutMillis::new(u64::from(MAX_TIMEOUT_MILLIS)).is_ok());
        assert!(TimeoutMillis::new(0).is_err());
        assert!(TimeoutMillis::new(u64::from(MAX_TIMEOUT_MILLIS) + 1).is_err());
    }

    #[test]
    fn every_count_and_byte_limit_rejects_invalid_boundaries() {
        assert!(MaxMessageBytes::new(0).is_err());
        assert!(MaxDedupEntries::new(0).is_err());
        assert!(MaxUnsyncedRecords::new(0).is_err());
        assert!(MaxOutcomeBytes::new(0).is_err());
        assert!(MaxTotalOutcomeBytes::new(u64::from(MIN_TOTAL_OUTCOME_BYTES)).is_ok());
        assert!(MaxTotalOutcomeBytes::new(u64::from(MIN_TOTAL_OUTCOME_BYTES - 1)).is_err());
        assert!(MaxTotalOutcomeBytes::new(u64::from(MAX_TOTAL_OUTCOME_BYTES) + 1).is_err());
    }
}
