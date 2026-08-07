use std::num::NonZeroU16;

use bytes::Bytes;
use iron_core::{ClientRequestId, Envelope, Lsn, SchemaVersion, TimerId};

use crate::ProtocolError;

/// Opaque client command delivered to a protocol instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientRequest {
    request_id: ClientRequestId,
    schema_version: SchemaVersion,
    command: Bytes,
}

impl ClientRequest {
    /// Creates a validated-identity client request with opaque command bytes.
    #[must_use]
    pub fn new(
        request_id: ClientRequestId,
        schema_version: SchemaVersion,
        command: impl Into<Bytes>,
    ) -> Self {
        Self {
            request_id,
            schema_version,
            command: command.into(),
        }
    }

    /// Returns the request identity.
    #[must_use]
    pub const fn request_id(&self) -> ClientRequestId {
        self.request_id
    }

    /// Returns the command schema version.
    #[must_use]
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    /// Returns the opaque command bytes.
    #[must_use]
    pub fn command(&self) -> &Bytes {
        &self.command
    }
}

/// A logical timer notification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimerFired {
    timer_id: TimerId,
}

impl TimerFired {
    /// Creates a timer notification.
    #[must_use]
    pub const fn new(timer_id: TimerId) -> Self {
        Self { timer_id }
    }

    /// Returns the timer identity.
    #[must_use]
    pub const fn timer_id(self) -> TimerId {
        self.timer_id
    }
}

/// One durable record supplied during recovery in ascending LSN order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredRecord {
    lsn: Lsn,
    record_kind: NonZeroU16,
    schema_version: SchemaVersion,
    payload: Bytes,
}

impl RecoveredRecord {
    /// Creates a recovery record after the runtime has validated WAL framing.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::InvalidValue`] for record kind zero.
    pub fn new(
        lsn: Lsn,
        record_kind: u16,
        schema_version: SchemaVersion,
        payload: impl Into<Bytes>,
    ) -> Result<Self, ProtocolError> {
        let record_kind = NonZeroU16::new(record_kind).ok_or(ProtocolError::InvalidValue {
            field: "record_kind",
            message: "must be nonzero",
        })?;
        Ok(Self {
            lsn,
            record_kind,
            schema_version,
            payload: payload.into(),
        })
    }

    /// Returns the WAL sequence number.
    #[must_use]
    pub const fn lsn(&self) -> Lsn {
        self.lsn
    }

    /// Returns the protocol-neutral record kind.
    #[must_use]
    pub const fn record_kind(&self) -> u16 {
        self.record_kind.get()
    }

    /// Returns the payload schema version.
    #[must_use]
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    /// Returns the opaque payload bytes.
    #[must_use]
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}

/// A deterministic input to a protocol state machine.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Event {
    /// A validated peer envelope.
    Message(Envelope),
    /// An opaque client command.
    ClientRequest(ClientRequest),
    /// A logical timer notification.
    TimerFired(TimerFired),
}
