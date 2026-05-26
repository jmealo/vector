//! pgoutput binary message parser.
//!
//! Parses the payload of an `XLogData` CopyData message into row-level events.
//! pgwire-replication already extracts `Begin` and `Commit` messages and emits
//! them as separate events, so here we only handle the row-level message
//! types: `Relation`, `Insert`, `Update`, `Delete`, `Truncate`, `Origin`,
//! `Type`.
//!
//! ## Wire format (`pgoutput`, proto_version = 1)
//!
//! Each `XLogData` payload starts with a one-byte message-type tag. The
//! relevant tags:
//!
//! | Tag | Message      | Notes                                            |
//! |-----|--------------|--------------------------------------------------|
//! | `R` | Relation     | Schema metadata for a relation OID               |
//! | `I` | Insert       | New row in a relation                            |
//! | `U` | Update       | Updated row (and optionally the previous row)    |
//! | `D` | Delete       | Deleted row (or its key)                         |
//! | `T` | Truncate     | One or more relations were truncated             |
//! | `O` | Origin       | Replication origin marker (informational)        |
//! | `Y` | Type         | Custom type info (informational)                 |
//!
//! ## Tuple-data encoding
//!
//! Inside Insert/Update/Delete the row is a TupleData block:
//!
//! ```text
//! Int16 num_columns
//!   per column:
//!     Int8 kind     'n' = null, 'u' = TOAST unchanged, 't' = text
//!     if kind == 't':
//!       Int32 length
//!       Bytes(length) text-encoded value
//! ```
//!
//! With `proto_version = 1` and the default settings PostgreSQL emits all
//! column values in TEXT format regardless of the underlying type.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use snafu::Snafu;
use vector_lib::config::{LegacyKey, LogNamespace};
use vector_lib::event::{KeyString, LogEvent, ObjectMap, Value};
use vrl::path;

use crate::sources::postgresql_cdc::config::PostgresqlCdcConfig;

/// Operation kind emitted as the `operation` field on each LogEvent.
#[derive(Debug, Clone, Copy)]
pub(super) enum Operation {
    Insert,
    Update,
    Delete,
    Truncate,
}

impl Operation {
    const fn as_str(self) -> &'static str {
        match self {
            Operation::Insert => "insert",
            Operation::Update => "update",
            Operation::Delete => "delete",
            Operation::Truncate => "truncate",
        }
    }
}

/// Information about a relation, cached by its OID and updated whenever the
/// server emits a new `Relation` message.
#[derive(Debug, Clone)]
struct RelationInfo {
    schema: Arc<str>,
    table: Arc<str>,
    columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone)]
struct ColumnInfo {
    name: Arc<str>,
    /// PostgreSQL type OID (e.g. 23 for int4). Used to choose a Vector
    /// `Value` variant when decoding column text.
    type_oid: u32,
    /// `flags & 1 != 0` indicates the column is part of the relation's
    /// replica-identity key. We keep the raw flags so we can surface this in
    /// the future if needed; for now it isn't used by the decoder.
    #[allow(dead_code)]
    flags: u8,
}

/// Errors raised while parsing a pgoutput XLogData payload.
#[derive(Debug, Snafu)]
pub(super) enum PgoutputError {
    #[snafu(display("truncated pgoutput message"))]
    Truncated,
    #[snafu(display("invalid utf-8 in cstring: {source}"))]
    InvalidUtf8 { source: std::string::FromUtf8Error },
    #[snafu(display("unknown relation OID {oid} (Relation message missing before row)"))]
    UnknownRelation { oid: u32 },
    #[snafu(display("row message arrived outside of any transaction"))]
    RowOutsideTransaction,
    #[snafu(display("tuple data kind byte {kind:?} is not one of 'n' / 'u' / 't'"))]
    BadTupleKind { kind: u8 },
    #[snafu(display("UPDATE message tuple-marker byte {kind:?} is not one of 'K' / 'O' / 'N'"))]
    BadUpdateMarker { kind: u8 },
    #[snafu(display(
        "UPDATE message new-tuple marker is {kind:?}; expected 'N' after a 'K'/'O' old tuple"
    ))]
    MissingUpdateNewMarker { kind: u8 },
    #[snafu(display("DELETE message tuple-marker byte {kind:?} is not one of 'K' / 'O'"))]
    BadDeleteMarker { kind: u8 },
}

impl From<std::string::FromUtf8Error> for PgoutputError {
    fn from(source: std::string::FromUtf8Error) -> Self {
        PgoutputError::InvalidUtf8 { source }
    }
}

/// Per-transaction metadata applied to every row event emitted within that
/// transaction. pgoutput guarantees non-interleaved transaction delivery, so
/// at most one transaction is open at any time. `Copy` lets the replication
/// loop snapshot the current transaction's metadata once per XLogData
/// without re-allocating the `DateTime`.
#[derive(Debug, Clone, Copy)]
pub(super) struct TxMeta {
    pub(super) final_lsn: u64,
    pub(super) transaction_id: u32,
    pub(super) commit_timestamp: DateTime<Utc>,
}

/// One row event produced by parsing a single pgoutput row message.
#[derive(Debug)]
pub(super) struct RowEvent {
    pub(super) log: LogEvent,
}

/// Outcome of parsing one XLogData payload.
#[derive(Debug)]
pub(super) enum Parsed {
    /// Zero or more row events were produced (Insert/Update/Delete/Truncate).
    Rows(Vec<RowEvent>),
    /// The message updated the relation cache or was otherwise informational.
    NoEvent,
}

/// pgoutput parser state — owns the relation cache and produces row events.
pub(super) struct PgoutputParser {
    relations: HashMap<u32, RelationInfo>,
}

impl PgoutputParser {
    pub(super) fn new() -> Self {
        Self {
            relations: HashMap::new(),
        }
    }

    /// Parses a single XLogData payload into zero or more row events.
    ///
    /// `tx_meta` carries the LSN, xid, and commit timestamp from the most
    /// recent Begin. Row messages outside a transaction return an error.
    pub(super) fn parse(
        &mut self,
        payload: &Bytes,
        tx_meta: Option<&TxMeta>,
        log_namespace: LogNamespace,
    ) -> Result<Parsed, PgoutputError> {
        let mut buf = payload.as_ref();
        if buf.is_empty() {
            return Ok(Parsed::NoEvent);
        }
        let tag = read_u8(&mut buf)?;
        match tag {
            b'R' => {
                self.parse_relation(buf)?;
                Ok(Parsed::NoEvent)
            }
            b'I' => {
                let tx = tx_meta.ok_or(PgoutputError::RowOutsideTransaction)?;
                let event = self.parse_insert(buf, tx, log_namespace)?;
                Ok(Parsed::Rows(vec![event]))
            }
            b'U' => {
                let tx = tx_meta.ok_or(PgoutputError::RowOutsideTransaction)?;
                let event = self.parse_update(buf, tx, log_namespace)?;
                Ok(Parsed::Rows(vec![event]))
            }
            b'D' => {
                let tx = tx_meta.ok_or(PgoutputError::RowOutsideTransaction)?;
                let event = self.parse_delete(buf, tx, log_namespace)?;
                Ok(Parsed::Rows(vec![event]))
            }
            b'T' => {
                let tx = tx_meta.ok_or(PgoutputError::RowOutsideTransaction)?;
                let events = self.parse_truncate(buf, tx, log_namespace)?;
                Ok(Parsed::Rows(events))
            }
            b'O' | b'Y' => {
                // Origin and Type messages are informational metadata that
                // do not carry row data; the row decoder has no use for them.
                Ok(Parsed::NoEvent)
            }
            other => {
                // Unknown message — log at debug; the upstream caller will see
                // NoEvent and continue.
                tracing::debug!(message = "Skipping unknown pgoutput message", tag = ?other);
                Ok(Parsed::NoEvent)
            }
        }
    }

    fn parse_relation(&mut self, mut buf: &[u8]) -> Result<(), PgoutputError> {
        let oid = read_u32(&mut buf)?;
        let schema = read_cstring(&mut buf)?;
        let table = read_cstring(&mut buf)?;
        // replica-identity setting — not used directly here, but consume it.
        let _replica_identity = read_u8(&mut buf)?;
        let num_columns = read_u16(&mut buf)? as usize;
        let mut columns = Vec::with_capacity(num_columns);
        for _ in 0..num_columns {
            let flags = read_u8(&mut buf)?;
            let name = read_cstring(&mut buf)?;
            let type_oid = read_u32(&mut buf)?;
            // atttypmod — consume but ignore for our purposes.
            let _atttypmod = read_u32(&mut buf)?;
            columns.push(ColumnInfo {
                name: Arc::from(name),
                type_oid,
                flags,
            });
        }
        self.relations.insert(
            oid,
            RelationInfo {
                schema: Arc::from(schema),
                table: Arc::from(table),
                columns,
            },
        );
        Ok(())
    }

    fn parse_insert(
        &self,
        mut buf: &[u8],
        tx: &TxMeta,
        log_namespace: LogNamespace,
    ) -> Result<RowEvent, PgoutputError> {
        let oid = read_u32(&mut buf)?;
        let rel = self
            .relations
            .get(&oid)
            .ok_or(PgoutputError::UnknownRelation { oid })?;
        // The tuple type byte is always 'N' for Insert; consume it.
        let _kind = read_u8(&mut buf)?;
        let (new_obj, toast_omitted) = decode_tuple(&mut buf, rel)?;
        Ok(build_event(
            Operation::Insert,
            rel,
            tx,
            Some(new_obj),
            None,
            toast_omitted,
            log_namespace,
        ))
    }

    fn parse_update(
        &self,
        mut buf: &[u8],
        tx: &TxMeta,
        log_namespace: LogNamespace,
    ) -> Result<RowEvent, PgoutputError> {
        let oid = read_u32(&mut buf)?;
        let rel = self
            .relations
            .get(&oid)
            .ok_or(PgoutputError::UnknownRelation { oid })?;
        // First byte may be 'K' (key-only old tuple), 'O' (full old tuple),
        // or 'N' (new tuple, no old). 'K'/'O' is followed by an old tuple
        // and then an 'N' marker before the new tuple; 'N' alone means only
        // the new tuple follows. Any other byte indicates a server-side
        // protocol change we do not understand — halt rather than risk
        // misaligned reads against the rest of the buffer.
        let kind = read_u8(&mut buf)?;
        let (old_obj, old_toast) = match kind {
            b'K' | b'O' => {
                let (old, t) = decode_tuple(&mut buf, rel)?;
                let new_marker = read_u8(&mut buf)?;
                if new_marker != b'N' {
                    return Err(PgoutputError::MissingUpdateNewMarker { kind: new_marker });
                }
                (Some(old), t)
            }
            b'N' => (None, Vec::new()),
            other => return Err(PgoutputError::BadUpdateMarker { kind: other }),
        };
        let (new_obj, new_toast) = decode_tuple(&mut buf, rel)?;
        // Merge TOAST-omitted lists; columns may differ between old and new.
        let mut toast_omitted = old_toast;
        for col in new_toast {
            if !toast_omitted.contains(&col) {
                toast_omitted.push(col);
            }
        }
        Ok(build_event(
            Operation::Update,
            rel,
            tx,
            Some(new_obj),
            old_obj,
            toast_omitted,
            log_namespace,
        ))
    }

    fn parse_delete(
        &self,
        mut buf: &[u8],
        tx: &TxMeta,
        log_namespace: LogNamespace,
    ) -> Result<RowEvent, PgoutputError> {
        let oid = read_u32(&mut buf)?;
        let rel = self
            .relations
            .get(&oid)
            .ok_or(PgoutputError::UnknownRelation { oid })?;
        // 'K' = key-only old tuple, 'O' = full old tuple (depending on
        // REPLICA IDENTITY). Any other byte indicates a server-side protocol
        // change; halt rather than risk misaligned reads.
        let kind = read_u8(&mut buf)?;
        if kind != b'K' && kind != b'O' {
            return Err(PgoutputError::BadDeleteMarker { kind });
        }
        let (old_obj, toast_omitted) = decode_tuple(&mut buf, rel)?;
        Ok(build_event(
            Operation::Delete,
            rel,
            tx,
            None,
            Some(old_obj),
            toast_omitted,
            log_namespace,
        ))
    }

    fn parse_truncate(
        &self,
        mut buf: &[u8],
        tx: &TxMeta,
        log_namespace: LogNamespace,
    ) -> Result<Vec<RowEvent>, PgoutputError> {
        let num_relations = read_u32(&mut buf)? as usize;
        // option flags (CASCADE / RESTART IDENTITY) — consume.
        let _flags = read_u8(&mut buf)?;
        let mut events = Vec::with_capacity(num_relations);
        for _ in 0..num_relations {
            let oid = read_u32(&mut buf)?;
            // Unknown-OID handling for TRUNCATE deliberately differs from
            // INSERT/UPDATE/DELETE: a TRUNCATE message bundles many
            // relations in a single payload, and skipping an unknown one
            // still lets us emit events for the known relations in the same
            // batch. INSERT/UPDATE/DELETE carry only one relation each and
            // cannot recover from a missing schema mapping, so they halt
            // with `UnknownRelation`. The mismatch is intentional.
            let rel = match self.relations.get(&oid) {
                Some(rel) => rel,
                None => {
                    tracing::debug!(message = "TRUNCATE on unknown relation OID", oid = oid);
                    continue;
                }
            };
            events.push(build_event(
                Operation::Truncate,
                rel,
                tx,
                None,
                None,
                Vec::new(),
                log_namespace,
            ));
        }
        Ok(events)
    }
}

/// Builds a LogEvent from the pieces produced by row-level parsing.
///
/// Field placement honours `log_namespace`: under `Legacy` every field
/// lands at the root of the `LogEvent` (the original shape); under
/// `Vector` every field lands under `%postgresql_cdc.<field>` event
/// metadata. The `LegacyKey::Overwrite` calls below intentionally mirror
/// the pre-namespace insertion behaviour for backwards compatibility.
fn build_event(
    operation: Operation,
    rel: &RelationInfo,
    tx: &TxMeta,
    new: Option<ObjectMap>,
    old: Option<ObjectMap>,
    toast_omitted: Vec<Arc<str>>,
    log_namespace: LogNamespace,
) -> RowEvent {
    let mut log = LogEvent::default();
    log_namespace.insert_source_metadata(
        PostgresqlCdcConfig::NAME,
        &mut log,
        Some(LegacyKey::Overwrite(path!("operation"))),
        path!("operation"),
        operation.as_str(),
    );
    log_namespace.insert_source_metadata(
        PostgresqlCdcConfig::NAME,
        &mut log,
        Some(LegacyKey::Overwrite(path!("schema"))),
        path!("schema"),
        Value::from(rel.schema.as_ref()),
    );
    log_namespace.insert_source_metadata(
        PostgresqlCdcConfig::NAME,
        &mut log,
        Some(LegacyKey::Overwrite(path!("table"))),
        path!("table"),
        Value::from(rel.table.as_ref()),
    );
    log_namespace.insert_source_metadata(
        PostgresqlCdcConfig::NAME,
        &mut log,
        Some(LegacyKey::Overwrite(path!("lsn"))),
        path!("lsn"),
        format_lsn(tx.final_lsn),
    );
    log_namespace.insert_source_metadata(
        PostgresqlCdcConfig::NAME,
        &mut log,
        Some(LegacyKey::Overwrite(path!("transaction_id"))),
        path!("transaction_id"),
        Value::Integer(i64::from(tx.transaction_id)),
    );
    log_namespace.insert_source_metadata(
        PostgresqlCdcConfig::NAME,
        &mut log,
        Some(LegacyKey::Overwrite(path!("transaction_timestamp"))),
        path!("transaction_timestamp"),
        Value::from(tx.commit_timestamp),
    );
    log_namespace.insert_source_metadata(
        PostgresqlCdcConfig::NAME,
        &mut log,
        Some(LegacyKey::Overwrite(path!("new"))),
        path!("new"),
        new.map(Value::Object).unwrap_or(Value::Null),
    );
    log_namespace.insert_source_metadata(
        PostgresqlCdcConfig::NAME,
        &mut log,
        Some(LegacyKey::Overwrite(path!("old"))),
        path!("old"),
        old.map(Value::Object).unwrap_or(Value::Null),
    );
    if !toast_omitted.is_empty() {
        let omitted: Vec<Value> = toast_omitted
            .into_iter()
            .map(|s| Value::from(s.as_ref()))
            .collect();
        log_namespace.insert_source_metadata(
            PostgresqlCdcConfig::NAME,
            &mut log,
            Some(LegacyKey::Overwrite(path!("__toast_omitted"))),
            path!("toast_omitted"),
            Value::Array(omitted),
        );
    }
    log_namespace.insert_standard_vector_source_metadata(
        &mut log,
        PostgresqlCdcConfig::NAME,
        Utc::now(),
    );
    RowEvent { log }
}

/// Decodes a tuple-data block, returning the object map of column values plus
/// the list of columns omitted as `UnchangedToastDatum`. Column names are
/// `Arc<str>` references into the `RelationInfo` cache — no heap allocation
/// per column per row.
fn decode_tuple(
    buf: &mut &[u8],
    rel: &RelationInfo,
) -> Result<(ObjectMap, Vec<Arc<str>>), PgoutputError> {
    let num_columns = read_u16(buf)? as usize;
    let mut obj = ObjectMap::new();
    let mut toast_omitted = Vec::new();
    for i in 0..num_columns {
        let kind = read_u8(buf)?;
        let col = rel.columns.get(i);
        match kind {
            b'n' => {
                if let Some(col) = col {
                    obj.insert(KeyString::from(col.name.as_ref()), Value::Null);
                }
            }
            b'u' => {
                if let Some(col) = col {
                    toast_omitted.push(Arc::clone(&col.name));
                }
            }
            b't' => {
                let len = read_u32(buf)? as usize;
                if buf.len() < len {
                    return Err(PgoutputError::Truncated);
                }
                let (value_bytes, rest) = buf.split_at(len);
                let value_bytes = value_bytes.to_vec();
                *buf = rest;
                if let Some(col) = col {
                    let value = decode_value(col.type_oid, value_bytes);
                    obj.insert(KeyString::from(col.name.as_ref()), value);
                }
            }
            other => return Err(PgoutputError::BadTupleKind { kind: other }),
        }
    }
    Ok((obj, toast_omitted))
}

/// Maps a column's raw TEXT-format bytes to a Vector `Value` based on the
/// column's PostgreSQL type OID.
///
/// Type mapping:
/// - text/varchar/char/name → `Value::Bytes`
/// - int2/int4/int8 → `Value::Integer`
/// - float4/float8 → `Value::Float`
/// - numeric → `Value::Bytes` (preserve exact precision; do NOT cast to f64)
/// - bool → `Value::Boolean`
/// - bytea → `Value::Bytes` of the decoded raw bytes
/// - json/jsonb → `Value::Object` (parsed)
/// - date/timestamp/timestamptz → `Value::Bytes` (ISO8601)
/// - other → `Value::Bytes`
fn decode_value(type_oid: u32, bytes: Vec<u8>) -> Value {
    // Postgres type OIDs that are stable in src/include/catalog/pg_type.dat.
    const BOOL: u32 = 16;
    const BYTEA: u32 = 17;
    const NAME: u32 = 19;
    const INT8: u32 = 20;
    const INT2: u32 = 21;
    const INT4: u32 = 23;
    const TEXT: u32 = 25;
    const JSON: u32 = 114;
    const FLOAT4: u32 = 700;
    const FLOAT8: u32 = 701;
    const BPCHAR: u32 = 1042;
    const VARCHAR: u32 = 1043;
    const NUMERIC: u32 = 1700;
    const JSONB: u32 = 3802;

    match type_oid {
        BOOL => match bytes.as_slice() {
            b"t" => Value::Boolean(true),
            b"f" => Value::Boolean(false),
            _ => Value::Bytes(bytes.into()),
        },
        INT2 | INT4 | INT8 => parse_integer(&bytes).unwrap_or_else(|| Value::Bytes(bytes.into())),
        FLOAT4 | FLOAT8 => parse_float(&bytes).unwrap_or_else(|| Value::Bytes(bytes.into())),
        BYTEA => Value::Bytes(decode_bytea(&bytes).into()),
        JSON | JSONB => parse_json(&bytes).unwrap_or_else(|| Value::Bytes(bytes.into())),
        // NUMERIC, TEXT-like, dates/timestamps, and everything else stay as
        // Bytes — VRL transforms can refine downstream.
        NUMERIC | TEXT | VARCHAR | BPCHAR | NAME => Value::Bytes(bytes.into()),
        _ => Value::Bytes(bytes.into()),
    }
}

fn parse_integer(bytes: &[u8]) -> Option<Value> {
    let s = std::str::from_utf8(bytes).ok()?;
    s.parse::<i64>().ok().map(Value::Integer)
}

fn parse_float(bytes: &[u8]) -> Option<Value> {
    let s = std::str::from_utf8(bytes).ok()?;
    let parsed = s.parse::<f64>().ok()?;
    // Reject both NaN and ±Infinity. NaN is forbidden by `Value::Float`'s
    // `NotNan` wrapper; Infinity is technically representable but downstream
    // JSON-based sinks cannot encode it cleanly. Falling back to the raw
    // text gives consumers a string they can pattern-match deterministically.
    if !parsed.is_finite() {
        return None;
    }
    ordered_float::NotNan::new(parsed).ok().map(Value::Float)
}

fn parse_json(bytes: &[u8]) -> Option<Value> {
    let parsed: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    Some(Value::from(parsed))
}

/// Decodes Postgres' default text-format bytea representation.
///
/// Modern Postgres defaults to `\x...` hex encoding, which is decoded to
/// raw bytes. Legacy escape format (`\\NNN` octal escapes) is passed through
/// unchanged; users on legacy configurations can decode in a VRL transform.
/// Anything that fails to decode is returned as-is so the user can still
/// see the raw bytes.
fn decode_bytea(bytes: &[u8]) -> Vec<u8> {
    if bytes.starts_with(b"\\x") {
        // Hex format: \x<hex...>. Odd-length hex is malformed (PG never
        // emits it) — fall back to the raw escape so the user sees the
        // corruption directly rather than silently losing the trailing
        // nibble.
        let hex = &bytes[2..];
        if !hex.len().is_multiple_of(2) {
            return bytes.to_vec();
        }
        let mut out = Vec::with_capacity(hex.len() / 2);
        let mut iter = hex.iter().copied();
        while let (Some(h), Some(l)) = (iter.next(), iter.next()) {
            match (hex_digit(h), hex_digit(l)) {
                (Some(h), Some(l)) => out.push((h << 4) | l),
                _ => return bytes.to_vec(),
            }
        }
        out
    } else {
        // Escape format — pass through unchanged. Users on legacy
        // configurations can decode in a VRL transform if needed.
        bytes.to_vec()
    }
}

const fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Formats an LSN as Postgres' `X/Y` uppercase-hex representation.
pub(super) fn format_lsn(lsn: u64) -> String {
    let hi = (lsn >> 32) as u32;
    let lo = (lsn & 0xFFFF_FFFF) as u32;
    format!("{:X}/{:X}", hi, lo)
}

// --- Low-level binary readers ---------------------------------------------

fn read_u8(buf: &mut &[u8]) -> Result<u8, PgoutputError> {
    if buf.is_empty() {
        return Err(PgoutputError::Truncated);
    }
    let v = buf[0];
    *buf = &buf[1..];
    Ok(v)
}

fn read_u16(buf: &mut &[u8]) -> Result<u16, PgoutputError> {
    if buf.len() < 2 {
        return Err(PgoutputError::Truncated);
    }
    let v = (&buf[..2]).get_u16();
    *buf = &buf[2..];
    Ok(v)
}

fn read_u32(buf: &mut &[u8]) -> Result<u32, PgoutputError> {
    if buf.len() < 4 {
        return Err(PgoutputError::Truncated);
    }
    let v = (&buf[..4]).get_u32();
    *buf = &buf[4..];
    Ok(v)
}

fn read_cstring(buf: &mut &[u8]) -> Result<String, PgoutputError> {
    let pos = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or(PgoutputError::Truncated)?;
    let s = String::from_utf8(buf[..pos].to_vec())?;
    *buf = &buf[pos + 1..];
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn tx_meta() -> TxMeta {
        TxMeta {
            final_lsn: 0x1_2345_6789,
            transaction_id: 42,
            commit_timestamp: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        }
    }

    /// Builds a minimal Relation message for `public.users(id int4, name text)`.
    fn relation_bytes() -> Bytes {
        let mut b = Vec::new();
        b.push(b'R');
        b.extend_from_slice(&100u32.to_be_bytes()); // OID
        b.extend_from_slice(b"public\0");
        b.extend_from_slice(b"users\0");
        b.push(b'd'); // replica identity = default
        b.extend_from_slice(&2u16.to_be_bytes()); // 2 columns
        // id int4 NOT NULL (flags=1 = key)
        b.push(1);
        b.extend_from_slice(b"id\0");
        b.extend_from_slice(&23u32.to_be_bytes()); // int4
        b.extend_from_slice(&u32::MAX.to_be_bytes()); // atttypmod
        // name text
        b.push(0);
        b.extend_from_slice(b"name\0");
        b.extend_from_slice(&25u32.to_be_bytes()); // text
        b.extend_from_slice(&u32::MAX.to_be_bytes());
        Bytes::from(b)
    }

    fn insert_bytes() -> Bytes {
        let mut b = Vec::new();
        b.push(b'I');
        b.extend_from_slice(&100u32.to_be_bytes()); // OID
        b.push(b'N');
        b.extend_from_slice(&2u16.to_be_bytes()); // 2 columns
        // id = 42
        b.push(b't');
        b.extend_from_slice(&2u32.to_be_bytes());
        b.extend_from_slice(b"42");
        // name = 'alice'
        b.push(b't');
        b.extend_from_slice(&5u32.to_be_bytes());
        b.extend_from_slice(b"alice");
        Bytes::from(b)
    }

    #[test]
    fn insert_in_vector_namespace_places_fields_under_metadata() {
        // Under the Vector log namespace, every CDC field must land under
        // `%postgresql_cdc.<field>` event metadata rather than at the root
        // of the LogEvent. This locks the namespace plumbing in place — a
        // regression that bypasses `insert_source_metadata` would put the
        // fields back at the root and break schema validators that expect
        // the Vector-namespace shape.
        let mut p = PgoutputParser::new();
        let tx = tx_meta();
        p.parse(&relation_bytes(), Some(&tx), LogNamespace::Vector)
            .unwrap();
        let parsed = p
            .parse(&insert_bytes(), Some(&tx), LogNamespace::Vector)
            .unwrap();
        let log = match parsed {
            Parsed::Rows(events) => events.into_iter().next().unwrap().log,
            Parsed::NoEvent => panic!("expected row event"),
        };

        // Root must NOT contain the CDC fields under Vector namespace.
        assert!(log.get("operation").is_none(), "operation leaked to root");
        assert!(log.get("new").is_none(), "new leaked to root");
        assert!(log.get("schema").is_none(), "schema leaked to root");

        // They must be present under %postgresql_cdc.<field>.
        let meta = log.metadata().value();
        assert_eq!(
            meta.get(path!("postgresql_cdc", "operation")).unwrap(),
            &Value::from("insert")
        );
        assert_eq!(
            meta.get(path!("postgresql_cdc", "schema")).unwrap(),
            &Value::from("public")
        );
        assert_eq!(
            meta.get(path!("postgresql_cdc", "table")).unwrap(),
            &Value::from("users")
        );
        match meta.get(path!("postgresql_cdc", "new")).unwrap() {
            Value::Object(map) => {
                assert_eq!(map.get("id").unwrap(), &Value::Integer(42));
                assert_eq!(map.get("name").unwrap(), &Value::from("alice"));
            }
            other => panic!("expected `new` Object under metadata, got {other:?}"),
        }
    }

    #[test]
    fn relation_then_insert() {
        let mut p = PgoutputParser::new();
        let tx = tx_meta();
        assert!(matches!(
            p.parse(&relation_bytes(), Some(&tx), LogNamespace::Legacy)
                .unwrap(),
            Parsed::NoEvent
        ));
        let parsed = p
            .parse(&insert_bytes(), Some(&tx), LogNamespace::Legacy)
            .unwrap();
        match parsed {
            Parsed::Rows(events) => {
                assert_eq!(events.len(), 1);
                let log = &events[0].log;
                assert_eq!(log.get("operation").unwrap(), &Value::from("insert"));
                assert_eq!(log.get("schema").unwrap(), &Value::from("public"));
                assert_eq!(log.get("table").unwrap(), &Value::from("users"));
                assert_eq!(log.get("transaction_id").unwrap(), &Value::Integer(42));
                assert_eq!(
                    log.get("lsn").unwrap(),
                    &Value::from(format_lsn(0x1_2345_6789))
                );
                let new = log.get("new").unwrap();
                match new {
                    Value::Object(map) => {
                        assert_eq!(map.get("id").unwrap(), &Value::Integer(42));
                        assert_eq!(map.get("name").unwrap(), &Value::from("alice"));
                    }
                    _ => panic!("expected Object for 'new'"),
                }
                assert_eq!(log.get("old").unwrap(), &Value::Null);
            }
            Parsed::NoEvent => panic!("expected row event"),
        }
    }

    #[test]
    fn insert_without_relation_errors() {
        let mut p = PgoutputParser::new();
        let tx = tx_meta();
        let result = p.parse(&insert_bytes(), Some(&tx), LogNamespace::Legacy);
        assert!(matches!(
            result,
            Err(PgoutputError::UnknownRelation { oid: 100 })
        ));
    }

    #[test]
    fn row_outside_tx_errors() {
        let mut p = PgoutputParser::new();
        p.parse(&relation_bytes(), None, LogNamespace::Legacy)
            .unwrap();
        let result = p.parse(&insert_bytes(), None, LogNamespace::Legacy);
        assert!(matches!(result, Err(PgoutputError::RowOutsideTransaction)));
    }

    #[test]
    fn update_with_unknown_marker_errors() {
        // Per pgoutput spec the first marker byte must be 'K', 'O', or 'N'.
        // A different byte indicates a server-side protocol change we do not
        // understand — halt rather than risk a misaligned read.
        let mut p = PgoutputParser::new();
        let tx = tx_meta();
        p.parse(&relation_bytes(), Some(&tx), LogNamespace::Legacy)
            .unwrap();
        let mut b = Vec::new();
        b.push(b'U');
        b.extend_from_slice(&100u32.to_be_bytes());
        b.push(b'X'); // bogus marker
        let result = p.parse(&Bytes::from(b), Some(&tx), LogNamespace::Legacy);
        assert!(
            matches!(result, Err(PgoutputError::BadUpdateMarker { kind: b'X' })),
            "expected BadUpdateMarker, got {result:?}"
        );
    }

    #[test]
    fn update_missing_new_marker_after_key_errors() {
        // After a 'K' (or 'O') old tuple, the next byte MUST be 'N' before
        // the new tuple. A non-'N' byte means we have misread the payload
        // or the server emitted an unsupported form — halt cleanly.
        let mut p = PgoutputParser::new();
        let tx = tx_meta();
        p.parse(&relation_bytes(), Some(&tx), LogNamespace::Legacy)
            .unwrap();
        let mut b = Vec::new();
        b.push(b'U');
        b.extend_from_slice(&100u32.to_be_bytes());
        b.push(b'K'); // key-only old tuple
        // Minimal old tuple: 2 columns, id=1, name=NULL.
        b.extend_from_slice(&2u16.to_be_bytes());
        b.push(b't');
        b.extend_from_slice(&1u32.to_be_bytes());
        b.push(b'1');
        b.push(b'n');
        b.push(b'X'); // bogus new-marker (should be 'N')
        let result = p.parse(&Bytes::from(b), Some(&tx), LogNamespace::Legacy);
        assert!(
            matches!(
                result,
                Err(PgoutputError::MissingUpdateNewMarker { kind: b'X' })
            ),
            "expected MissingUpdateNewMarker, got {result:?}"
        );
    }

    #[test]
    fn delete_with_unknown_marker_errors() {
        // DELETE message tuple-marker must be 'K' or 'O'.
        let mut p = PgoutputParser::new();
        let tx = tx_meta();
        p.parse(&relation_bytes(), Some(&tx), LogNamespace::Legacy)
            .unwrap();
        let mut b = Vec::new();
        b.push(b'D');
        b.extend_from_slice(&100u32.to_be_bytes());
        b.push(b'Z'); // bogus
        let result = p.parse(&Bytes::from(b), Some(&tx), LogNamespace::Legacy);
        assert!(
            matches!(result, Err(PgoutputError::BadDeleteMarker { kind: b'Z' })),
            "expected BadDeleteMarker, got {result:?}"
        );
    }

    #[test]
    fn null_column_is_value_null() {
        let mut p = PgoutputParser::new();
        let tx = tx_meta();
        p.parse(&relation_bytes(), Some(&tx), LogNamespace::Legacy)
            .unwrap();
        let mut b = Vec::new();
        b.push(b'I');
        b.extend_from_slice(&100u32.to_be_bytes());
        b.push(b'N');
        b.extend_from_slice(&2u16.to_be_bytes());
        b.push(b't');
        b.extend_from_slice(&1u32.to_be_bytes());
        b.push(b'7');
        b.push(b'n'); // NULL
        let payload = Bytes::from(b);
        let parsed = p.parse(&payload, Some(&tx), LogNamespace::Legacy).unwrap();
        match parsed {
            Parsed::Rows(events) => match events[0].log.get("new").unwrap() {
                Value::Object(map) => {
                    assert_eq!(map.get("id").unwrap(), &Value::Integer(7));
                    assert_eq!(map.get("name").unwrap(), &Value::Null);
                }
                _ => panic!(),
            },
            _ => panic!(),
        }
    }

    #[test]
    fn toast_unchanged_omits_column() {
        let mut p = PgoutputParser::new();
        let tx = tx_meta();
        p.parse(&relation_bytes(), Some(&tx), LogNamespace::Legacy)
            .unwrap();
        let mut b = Vec::new();
        b.push(b'I');
        b.extend_from_slice(&100u32.to_be_bytes());
        b.push(b'N');
        b.extend_from_slice(&2u16.to_be_bytes());
        b.push(b't');
        b.extend_from_slice(&1u32.to_be_bytes());
        b.push(b'7');
        b.push(b'u'); // TOAST unchanged
        let payload = Bytes::from(b);
        let parsed = p.parse(&payload, Some(&tx), LogNamespace::Legacy).unwrap();
        match parsed {
            Parsed::Rows(events) => {
                let log = &events[0].log;
                let omitted = log.get("__toast_omitted").unwrap();
                match omitted {
                    Value::Array(arr) => {
                        assert_eq!(arr.len(), 1);
                        assert_eq!(arr[0], Value::from("name"));
                    }
                    _ => panic!(),
                }
                let new = log.get("new").unwrap();
                if let Value::Object(map) = new {
                    assert!(!map.contains_key("name"));
                } else {
                    panic!();
                }
            }
            _ => panic!(),
        }
    }

    #[test]
    fn schema_change_invalidates_relation_cache() {
        let mut p = PgoutputParser::new();
        let tx = tx_meta();
        p.parse(&relation_bytes(), Some(&tx), LogNamespace::Legacy)
            .unwrap();

        // New Relation: same OID, now with an extra column.
        let mut b = Vec::new();
        b.push(b'R');
        b.extend_from_slice(&100u32.to_be_bytes());
        b.extend_from_slice(b"public\0");
        b.extend_from_slice(b"users\0");
        b.push(b'd');
        b.extend_from_slice(&3u16.to_be_bytes());
        b.push(1);
        b.extend_from_slice(b"id\0");
        b.extend_from_slice(&23u32.to_be_bytes());
        b.extend_from_slice(&u32::MAX.to_be_bytes());
        b.push(0);
        b.extend_from_slice(b"name\0");
        b.extend_from_slice(&25u32.to_be_bytes());
        b.extend_from_slice(&u32::MAX.to_be_bytes());
        b.push(0);
        b.extend_from_slice(b"email\0");
        b.extend_from_slice(&25u32.to_be_bytes());
        b.extend_from_slice(&u32::MAX.to_be_bytes());
        p.parse(&Bytes::from(b), Some(&tx), LogNamespace::Legacy)
            .unwrap();

        // Insert that includes the new column.
        let mut b = Vec::new();
        b.push(b'I');
        b.extend_from_slice(&100u32.to_be_bytes());
        b.push(b'N');
        b.extend_from_slice(&3u16.to_be_bytes());
        b.push(b't');
        b.extend_from_slice(&1u32.to_be_bytes());
        b.push(b'9');
        b.push(b't');
        b.extend_from_slice(&3u32.to_be_bytes());
        b.extend_from_slice(b"bob");
        b.push(b't');
        b.extend_from_slice(&5u32.to_be_bytes());
        b.extend_from_slice(b"b@b.c");
        let parsed = p
            .parse(&Bytes::from(b), Some(&tx), LogNamespace::Legacy)
            .unwrap();
        match parsed {
            Parsed::Rows(events) => {
                if let Value::Object(map) = events[0].log.get("new").unwrap() {
                    assert_eq!(map.get("email").unwrap(), &Value::from("b@b.c"));
                } else {
                    panic!();
                }
            }
            _ => panic!(),
        }
    }

    #[test]
    fn truncate_emits_one_event_per_relation() {
        let mut p = PgoutputParser::new();
        let tx = tx_meta();
        p.parse(&relation_bytes(), Some(&tx), LogNamespace::Legacy)
            .unwrap();

        let mut b = Vec::new();
        b.push(b'T');
        b.extend_from_slice(&1u32.to_be_bytes()); // 1 relation
        b.push(0); // no flags
        b.extend_from_slice(&100u32.to_be_bytes());
        let parsed = p
            .parse(&Bytes::from(b), Some(&tx), LogNamespace::Legacy)
            .unwrap();
        match parsed {
            Parsed::Rows(events) => {
                assert_eq!(events.len(), 1);
                let log = &events[0].log;
                assert_eq!(log.get("operation").unwrap(), &Value::from("truncate"));
                assert_eq!(log.get("table").unwrap(), &Value::from("users"));
                assert_eq!(log.get("new").unwrap(), &Value::Null);
                assert_eq!(log.get("old").unwrap(), &Value::Null);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn format_lsn_matches_postgres_display() {
        assert_eq!(format_lsn(0), "0/0");
        assert_eq!(format_lsn(0x16_B374_D848), "16/B374D848");
        assert_eq!(format_lsn(0xFFFF_FFFF_FFFF_FFFF), "FFFFFFFF/FFFFFFFF");
        // High part non-zero, low part zero — bit-shift sanity check.
        assert_eq!(format_lsn(0x1_0000_0000), "1/0");
    }

    // ---- decode_value table tests ---------------------------------------
    //
    // The wire-format text representation Postgres sends for each type is
    // pinned here. A regression in the OID branches of `decode_value` would
    // silently change the type of a user's column under them; these unit
    // tests catch that in milliseconds, without needing a Postgres instance.

    fn decode(oid: u32, bytes: &[u8]) -> Value {
        decode_value(oid, bytes.to_vec())
    }

    #[test]
    fn decode_value_bool() {
        assert_eq!(decode(16, b"t"), Value::Boolean(true));
        assert_eq!(decode(16, b"f"), Value::Boolean(false));
        // Anything other than "t"/"f" falls back to Bytes (PG would never
        // send this, but the fallback must be safe).
        assert_eq!(decode(16, b"yes"), Value::Bytes("yes".into()));
    }

    #[test]
    fn decode_value_integers() {
        assert_eq!(decode(21, b"42"), Value::Integer(42)); // int2
        assert_eq!(decode(23, b"-1"), Value::Integer(-1)); // int4
        assert_eq!(decode(20, b"9223372036854775807"), Value::Integer(i64::MAX)); // int8
        // Out-of-i64-range: fall back to Bytes preserving the exact value.
        assert_eq!(
            decode(20, b"9223372036854775808"),
            Value::Bytes("9223372036854775808".into())
        );
    }

    #[test]
    fn decode_value_floats() {
        match decode(700, b"3.14") {
            Value::Float(f) => assert!((f.into_inner() - 3.14).abs() < 1e-9),
            other => panic!("expected Float, got {other:?}"),
        }
        match decode(701, b"-0.5") {
            Value::Float(f) => assert!((f.into_inner() + 0.5).abs() < 1e-9),
            other => panic!("expected Float, got {other:?}"),
        }
        // NaN and Inf both fail NotNan::new and fall back to Bytes.
        assert_eq!(decode(701, b"NaN"), Value::Bytes("NaN".into()));
        assert_eq!(decode(701, b"Infinity"), Value::Bytes("Infinity".into()));
    }

    #[test]
    fn decode_value_numeric_preserved_exactly() {
        // The defining numeric property: arbitrary precision survives the
        // round-trip. Tests cover typical currency precision as well as
        // values f64 cannot represent exactly.
        for &s in &[
            "0",
            "1.00",
            "9999999999.9999",        // 14-digit currency
            "1.234567890123456789",   // 19 significant digits
            "-0.0001",                // sub-cent precision
            "12345678901234567890.5", // 21 digits — exceeds i64 and f64 mantissa
        ] {
            assert_eq!(decode(1700, s.as_bytes()), Value::Bytes(s.into()));
        }
    }

    #[test]
    fn decode_value_text_types() {
        for oid in [25, 1043, 1042, 19] {
            assert_eq!(decode(oid, b"hello"), Value::Bytes("hello".into()));
        }
        // UTF-8 multi-byte string in a text column.
        assert_eq!(
            decode(25, "héllo 世界".as_bytes()),
            Value::Bytes("héllo 世界".into())
        );
    }

    #[test]
    fn decode_value_bytea_hex() {
        // PG default: `\x` followed by hex.
        let bytea = b"\\xdeadbeef";
        match decode(17, bytea) {
            Value::Bytes(b) => assert_eq!(&b[..], &[0xDE, 0xAD, 0xBE, 0xEF][..]),
            other => panic!("expected Bytes, got {other:?}"),
        }
        // Empty hex.
        match decode(17, b"\\x") {
            Value::Bytes(b) => assert!(b.is_empty()),
            other => panic!("expected Bytes, got {other:?}"),
        }
        // Odd-length hex falls back to the raw bytes (defensible — PG would
        // never emit this, but we must not silently truncate).
        match decode(17, b"\\xabc") {
            Value::Bytes(b) => assert_eq!(&b[..], b"\\xabc"),
            other => panic!("expected Bytes, got {other:?}"),
        }
    }

    #[test]
    fn decode_value_json_and_jsonb() {
        for &oid in &[114u32, 3802] {
            let v = decode(oid, br#"{"id":42,"name":"alice"}"#);
            match v {
                Value::Object(map) => {
                    assert_eq!(map.get("id").unwrap(), &Value::Integer(42));
                    assert_eq!(map.get("name").unwrap(), &Value::from("alice"));
                }
                other => panic!("expected Object, got {other:?}"),
            }
        }
        // Malformed JSON falls back to the raw text.
        let v = decode(114, b"not json");
        assert_eq!(v, Value::Bytes("not json".into()));
    }

    #[test]
    fn decode_value_unknown_oid_falls_back_to_bytes() {
        // Any OID we don't special-case stays as opaque text. This is the
        // contract for UUID, inet, cidr, timestamps, intervals, ranges, etc.
        for oid in [2950u32, 869, 1184, 1186, 3904] {
            assert_eq!(decode(oid, b"opaque"), Value::Bytes("opaque".into()));
        }
    }
}
