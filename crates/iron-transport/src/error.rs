use std::error::Error;

use thiserror::Error;

/// A type-erased error that retains an adapter's typed source chain.
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// Failures at the transport delivery boundary.
///
/// Callers must branch on variants rather than matching display text. Retry
/// policy remains a runtime concern; [`TransportError::is_retryable`] is only
/// a classification of whether retry can ever be useful.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum TransportError {
    /// The adapter rejected an input before attempting delivery.
    #[error("invalid transport input: {message}")]
    InvalidInput {
        /// A bounded diagnostic that must not contain an entire payload.
        message: String,
    },

    /// The remote boundary could not currently be reached.
    #[error("transport unavailable")]
    Unavailable {
        /// The adapter-specific cause, when one exists.
        #[source]
        source: Option<BoxError>,
    },

    /// A bounded transport operation exceeded its configured deadline.
    #[error("transport operation `{operation}` timed out")]
    Timeout {
        /// The stable operation name.
        operation: &'static str,
    },

    /// Local capacity prevented acceptance.
    #[error("transport backpressure at capacity {capacity}")]
    Backpressure {
        /// The capacity associated with the rejection.
        capacity: usize,
    },

    /// The remote delivery boundary rejected the request.
    #[error("remote transport rejected the envelope ({code}): {message}")]
    RemoteRejected {
        /// A stable adapter or wire-protocol code.
        code: String,
        /// A bounded, non-secret diagnostic.
        message: String,
    },

    /// An unexpected adapter failure.
    #[error("internal transport failure")]
    Internal {
        /// The adapter-specific cause.
        #[source]
        source: BoxError,
    },
}

impl TransportError {
    /// Returns whether a future retry could plausibly succeed.
    ///
    /// This does not select a delay or authorize a retry. The future runtime
    /// owns retry limits, backoff, and envelope attempt increments.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Unavailable { .. } | Self::Timeout { .. } | Self::Backpressure { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::TransportError;

    #[test]
    fn retryability_is_variant_based() {
        assert!(TransportError::Timeout { operation: "send" }.is_retryable());
        assert!(
            TransportError::Unavailable { source: None }.is_retryable(),
            "temporary unavailability may be retried by a runtime"
        );
        assert!(
            !TransportError::RemoteRejected {
                code: "cluster-mismatch".to_owned(),
                message: "wrong cluster".to_owned(),
            }
            .is_retryable()
        );
    }
}
