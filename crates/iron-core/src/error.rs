use std::cmp::Ordering;
use std::fmt;
use std::num::ParseIntError;

use thiserror::Error;

/// An error produced while validating or operating on core values.
///
/// Callers should match variants, rather than classifying failures by their
/// display strings. The enum is non-exhaustive so that additional validation
/// failures can be represented without breaking callers.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CoreError {
    #[error("{kind} is not a valid UUID")]
    InvalidUuid {
        kind: &'static str,
        #[source]
        source: uuid::Error,
    },

    #[error("{kind} must not be all zeroes")]
    ZeroIdentifier { kind: &'static str },

    #[error("generated {kind} must not be all zeroes")]
    GeneratedZeroIdentifier { kind: &'static str },

    #[error("{kind} is invalid: {reason}")]
    InvalidText {
        kind: &'static str,
        reason: &'static str,
    },

    #[error("{kind} is not a valid integer")]
    InvalidInteger {
        kind: &'static str,
        #[source]
        source: ParseIntError,
    },

    #[error("{kind} must be nonzero")]
    ZeroValue { kind: &'static str },

    #[error("{kind} value {actual} is outside the accepted range {minimum}..={maximum}")]
    ValueOutOfRange {
        kind: &'static str,
        actual: u64,
        minimum: u64,
        maximum: u64,
    },

    #[error("peer endpoint is not a valid URL")]
    InvalidPeerEndpointUrl {
        #[source]
        source: url::ParseError,
    },

    #[error("peer endpoint scheme must be http or https")]
    InvalidPeerEndpointScheme,

    #[error("peer endpoint must contain a host")]
    MissingPeerEndpointHost,

    #[error("peer endpoint credentials are not permitted")]
    PeerEndpointCredentialsNotAllowed,

    #[error("peer endpoint query strings are not permitted")]
    PeerEndpointQueryNotAllowed,

    #[error("peer endpoint fragments are not permitted")]
    PeerEndpointFragmentNotAllowed,

    #[error("unsupported envelope version {version}")]
    UnsupportedEnvelopeVersion { version: u16 },

    #[error("envelope payload is {actual} bytes but the configured maximum is {maximum}")]
    PayloadTooLarge { actual: u64, maximum: u32 },

    #[error("delivery attempt overflow")]
    DeliveryAttemptOverflow,

    #[error("semantic fingerprint is invalid: {reason}")]
    InvalidFingerprint { reason: &'static str },

    #[error("{counter} sequence counter is exhausted")]
    SequenceExhausted { counter: &'static str },

    #[error("dedup outcome is {actual} bytes but the configured per-outcome maximum is {maximum}")]
    OutcomeTooLarge { actual: u64, maximum: u32 },

    #[error(transparent)]
    Validation(#[from] ValidationErrors),
}

/// One configuration validation problem.
///
/// Values created by core validation use stable, machine-readable `code`
/// strings and never copy the rejected value into `message`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationError {
    field_path: String,
    code: &'static str,
    message: &'static str,
}

impl ValidationError {
    /// Creates a validation problem from a path and stable static description.
    #[must_use]
    pub fn new(field_path: impl Into<String>, code: &'static str, message: &'static str) -> Self {
        Self {
            field_path: field_path.into(),
            code,
            message,
        }
    }

    #[must_use]
    pub fn field_path(&self) -> &str {
        &self.field_path
    }

    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    #[must_use]
    pub const fn message(&self) -> &'static str {
        self.message
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} [{}]: {}",
            self.field_path, self.code, self.message
        )
    }
}

/// A sorted, nonempty collection of configuration validation problems.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationErrors {
    errors: Box<[ValidationError]>,
}

impl ValidationErrors {
    /// Creates a nonempty collection and sorts it by field path and code.
    #[must_use]
    pub fn new(first: ValidationError, rest: impl IntoIterator<Item = ValidationError>) -> Self {
        let mut errors = vec![first];
        errors.extend(rest);
        Self::from_nonempty(errors)
    }

    pub(crate) fn from_vec(errors: Vec<ValidationError>) -> Option<Self> {
        if errors.is_empty() {
            None
        } else {
            Some(Self::from_nonempty(errors))
        }
    }

    fn from_nonempty(mut errors: Vec<ValidationError>) -> Self {
        errors.sort_by(|left, right| {
            let primary = left.field_path.cmp(&right.field_path);
            if primary != Ordering::Equal {
                return primary;
            }
            let secondary = left.code.cmp(right.code);
            if secondary != Ordering::Equal {
                return secondary;
            }
            left.message.cmp(right.message)
        });
        Self {
            errors: errors.into_boxed_slice(),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.errors.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn iter(&self) -> std::slice::Iter<'_, ValidationError> {
        self.errors.iter()
    }

    #[must_use]
    pub fn as_slice(&self) -> &[ValidationError] {
        &self.errors
    }

    #[must_use]
    pub fn into_vec(self) -> Vec<ValidationError> {
        self.errors.into_vec()
    }
}

impl fmt::Display for ValidationErrors {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "configuration validation failed with {} error(s)",
            self.errors.len()
        )
    }
}

impl std::error::Error for ValidationErrors {}

impl<'a> IntoIterator for &'a ValidationErrors {
    type Item = &'a ValidationError;
    type IntoIter = std::slice::Iter<'a, ValidationError>;

    fn into_iter(self) -> Self::IntoIter {
        self.errors.iter()
    }
}

impl IntoIterator for ValidationErrors {
    type Item = ValidationError;
    type IntoIter = std::vec::IntoIter<ValidationError>;

    fn into_iter(self) -> Self::IntoIter {
        self.errors.into_vec().into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::{ValidationError, ValidationErrors};

    #[test]
    fn validation_errors_are_nonempty_and_sorted() {
        let errors = ValidationErrors::new(
            ValidationError::new("z", "b", "last"),
            [
                ValidationError::new("a", "z", "second"),
                ValidationError::new("a", "a", "first"),
            ],
        );

        let order: Vec<_> = errors
            .iter()
            .map(|error| (error.field_path(), error.code()))
            .collect();
        assert_eq!(order, vec![("a", "a"), ("a", "z"), ("z", "b")]);
        assert!(!errors.is_empty());
    }
}
