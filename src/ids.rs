//! Strong-typed identifiers used throughout the worker.
//!
//! Each newtype wraps a primitive so the compiler rejects mix-ups
//! (`SessionId` into a slot expecting `PseudoId`, `Seq` vs `ChunkId`,
//! etc.). Display + Debug + Serialize / Deserialize are derived so these
//! drop directly into tracing fields and JSON payloads.
//!
//! The newtype pattern here exists for one reason: eliminate the
//! "which string goes where" bug class in the event loop where a WS
//! payload pushes five positional strings into an async chain.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

// --- SessionId -----------------------------------------------------------

/// A session UUID as returned by the data-api.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(pub Uuid);

impl SessionId {
    pub fn new(u: Uuid) -> Self { Self(u) }
    pub fn as_uuid(&self) -> Uuid { self.0 }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { self.0.fmt(f) }
}

impl fmt::Debug for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SessionId({})", self.0)
    }
}

impl From<Uuid> for SessionId {
    fn from(u: Uuid) -> Self { Self(u) }
}

// --- PseudoId ------------------------------------------------------------

/// A speaker pseudo_id (24-hex-char string per data-api schema).
///
/// The data-api enforces the format; the worker treats it as opaque.
#[derive(Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PseudoId(pub String);

impl PseudoId {
    pub fn new(s: impl Into<String>) -> Self { Self(s.into()) }
    pub fn as_str(&self) -> &str { &self.0 }
}

impl fmt::Display for PseudoId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { self.0.fmt(f) }
}

impl fmt::Debug for PseudoId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PseudoId({})", self.0)
    }
}

// --- Seq -----------------------------------------------------------------

/// Monotonic per-`(session_id, pseudo_id)` chunk sequence number.
#[derive(Copy, Clone, Eq, PartialEq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Seq(pub u32);

impl Seq {
    pub fn new(n: u32) -> Self { Self(n) }
    pub fn as_u32(&self) -> u32 { self.0 }
}

impl fmt::Display for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { self.0.fmt(f) }
}

impl fmt::Debug for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Seq({})", self.0)
    }
}

// --- ChunkId -------------------------------------------------------------

/// The full `(session_id, pseudo_id, seq)` triple that identifies one chunk.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct ChunkId {
    pub session: SessionId,
    pub pseudo: PseudoId,
    pub seq: Seq,
}

impl ChunkId {
    pub fn new(session: SessionId, pseudo: PseudoId, seq: Seq) -> Self {
        Self { session, pseudo, seq }
    }
}

impl fmt::Display for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.session, self.pseudo, self.seq)
    }
}
