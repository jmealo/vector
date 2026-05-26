//! Async replication loop for the `postgresql_cdc` source.
//!
//! ## Overview
//!
//! 1. **Setup connection** (`tokio_postgres`, non-replication) — validates
//!    that the configured slot exists, uses the `pgoutput` plugin, is not
//!    already attached to another consumer, and has WAL still retained. The
//!    connection honours the same TLS configuration as the replication
//!    connection; it would be a security regression to send the password
//!    cleartext while validating and then upgrade later.
//!
//! 2. **Replication client** (`pgwire_replication`) — opens a logical
//!    replication connection and streams events. pgwire handles framing,
//!    keepalives, and standby status updates internally; we feed it the
//!    confirmed LSN via [`ReplicationClient::update_applied_lsn`].
//!
//! 3. **Event loop** — receives [`ReplicationEvent`]s, maintains the current
//!    transaction's metadata, parses pgoutput row messages, attaches a
//!    [`BatchNotifier`] to each emitted event (when acknowledgements are
//!    enabled), and registers the resulting receiver with the
//!    [`LsnTracker`]. After each iteration the tracker is polled
//!    non-blockingly; when the confirmed LSN advances it is forwarded to the
//!    replication client.
//!
//! ## Security: password handling
//!
//! The libpq password lives temporarily in a `String` on the
//! [`pgwire_replication::ReplicationConfig`] we construct. That type derives
//! `Debug` and does not redact the password (see
//! [`build_replication_config`] for the safety contract). **Never** format
//! the resulting config with `{:?}` or `?cfg` anywhere — only debug-format
//! the public-safe fields (host, user, database).

use std::time::Duration;

use chrono::{TimeZone, Utc};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use pgwire_replication::{
    Lsn, ReplicationClient, ReplicationEvent,
    config::{ReplicationConfig as PgwireConfig, SslMode, TlsConfig as PgwireTlsConfig},
};
use postgres_openssl::MakeTlsConnector;
use snafu::Snafu;
use tokio::time::sleep;
use tokio_postgres::NoTls;
use vector_lib::{
    config::LogNamespace,
    event::{BatchNotifier, BatchStatusReceiver, Event, LogEvent, Value},
    shutdown::ShutdownSignal,
};

use vector_lib::{
    EstimatedJsonEncodedSizeOf,
    internal_event::{
        BytesReceived, CountByteSize, EventsReceived, InternalEventHandle as _, Protocol,
        Registered,
    },
};

use super::{
    config::{PostgresqlCdcConfig, PostgresqlCdcTlsConfig},
    lsn_tracker::LsnTracker,
    pgoutput::{Parsed, PgoutputParser, TxMeta, format_lsn},
};
use crate::{
    SourceSender,
    internal_events::{
        PostgresqlCdcParseError, PostgresqlCdcProtocolViolation, PostgresqlCdcReplicationError,
        PostgresqlCdcSinkAckFailed, StreamClosedError,
    },
};

/// Microseconds from the Unix epoch (1970-01-01) to the PostgreSQL epoch
/// (2000-01-01). Add this to a PG-epoch micros value to get Unix-epoch micros.
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// Maximum time the event loop blocks waiting on `client.recv()` before
/// polling the LSN tracker. This bounds how late a confirmed-flush update
/// is reported when the stream is otherwise idle.
///
/// **Derivation:** the lower bound is CPU cost of a `try_recv` poll loop
/// (negligible — no syscall, just an atomic load per pending LSN). The
/// upper bound is how much "extra" WAL PostgreSQL retains after a sink
/// ack lands but before we forward the new confirmed LSN: at 250ms the
/// server holds at most one quarter-second of already-consumed WAL,
/// which is insignificant relative to typical `wal_keep_size` budgets
/// (often GB-scale). Values in the 100ms–1s range are all reasonable;
/// 250ms was chosen as a middle ground. See the `postgresql_cdc-benchmarks`
/// feature for throughput tests at different intervals.
const TRACKER_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Cadence at which `pgwire-replication` sends a `StandbyStatusUpdate` to
/// the server.
///
/// **Derivation:** PostgreSQL's `wal_sender_timeout` (default 60s) kills
/// the replication connection if it sees no client activity for that long.
/// The status update must fire well under half that threshold (~30s) to
/// avoid spurious disconnects. `pgwire-replication`'s own default is 10s,
/// which is safe but means `confirmed_flush_lsn` only advances every 10s
/// even when the sink acks sub-second — making slot-lag dashboards look
/// stale. 500ms gives 120× safety margin against the 60s timeout while
/// providing near-real-time progress reporting. Values from 250ms to 2s
/// are all viable; the throughput difference is negligible because the
/// status update is a single 34-byte `StandbyStatusUpdate` message.
const STATUS_UPDATE_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Snafu)]
pub(super) enum BuildError {
    #[snafu(display("invalid connection_string: {source}"))]
    InvalidConnectionString { source: tokio_postgres::Error },
    #[snafu(display(
        "connection_string is missing the {field} component required for replication"
    ))]
    MissingConnectionField { field: &'static str },
    #[snafu(display(
        "password contains non-UTF-8 bytes; pgwire-replication requires a UTF-8 password"
    ))]
    NonUtf8Password,
    #[snafu(display("failed to build TLS connector: {message}"))]
    TlsBuild { message: String },
    #[snafu(display("failed to open setup connection: {source}"))]
    SetupConnect { source: tokio_postgres::Error },
    #[snafu(display(
        "replication slot {slot:?} does not exist on the server; create it with \
         SELECT pg_create_logical_replication_slot({slot:?}, 'pgoutput') before running this source"
    ))]
    SlotMissing { slot: String },
    #[snafu(display(
        "replication slot {slot:?} uses output_plugin={plugin:?}, but only 'pgoutput' is supported"
    ))]
    WrongOutputPlugin { slot: String, plugin: String },
    #[snafu(display(
        "replication slot {slot:?} is currently attached to another consumer (active=true). \
         Stop the other consumer or wait for it to disconnect before starting this source."
    ))]
    SlotAlreadyActive { slot: String },
    #[snafu(display(
        "replication slot {slot:?} reports wal_status={status:?}; the WAL required to resume \
         this slot has been removed by the server. Recreate the slot (which will resume from \
         the current write position) or restore from backup."
    ))]
    SlotWalLost { slot: String, status: String },
    #[snafu(display("invalid slot_start_lsn {lsn:?}: must be in 'X/Y' hex format"))]
    InvalidStartLsn { lsn: String },
    #[snafu(display("failed to query pg_replication_slots: {source}"))]
    QueryFailed { source: tokio_postgres::Error },
}

pub(super) async fn build(
    config: PostgresqlCdcConfig,
    cx: crate::config::SourceContext,
) -> crate::Result<crate::sources::Source> {
    // Eagerly parse the connection string so the user gets a clear error at
    // config-load time rather than after the source has started.
    let pg_config: tokio_postgres::Config = config
        .connection_string
        .inner()
        .parse()
        .map_err(|source| BuildError::InvalidConnectionString { source })?;

    let acknowledgements = cx.do_acknowledgements(config.acknowledgements);
    let log_namespace = cx.log_namespace(config.log_namespace);

    // Validate the slot *synchronously* during build() so misconfiguration
    // surfaces as a config error instead of as a source that starts then
    // immediately dies. The slot is validated over TLS if TLS is configured.
    validate_slot(&pg_config, &config.tls, &config.replication_slot).await?;

    let replication_config = build_replication_config(&pg_config, &config)?;

    Ok(Box::pin(stream_events(
        replication_config,
        cx.out,
        cx.shutdown,
        acknowledgements,
        log_namespace,
    )))
}

async fn validate_slot(
    pg_config: &tokio_postgres::Config,
    tls: &Option<PostgresqlCdcTlsConfig>,
    slot: &str,
) -> Result<(), BuildError> {
    // Connect using the same TLS posture the replication connection will
    // use. Sending the libpq password cleartext during validation would
    // defeat the user's explicit `tls.ca_file` configuration. The
    // `verify_hostname` setting must reach this connection too — otherwise
    // a user who set `verify_hostname = false` (to connect to a host whose
    // certificate does not list the address in its SAN) would succeed on
    // the replication path but fail here, because hostname verification on
    // `MakeTlsConnector` is per-connection state on `ConnectConfiguration`,
    // not on the builder.
    let (client, connection_handle) = match tls {
        Some(tls) => {
            let mut builder = SslConnector::builder(SslMethod::tls_client()).map_err(|e| {
                BuildError::TlsBuild {
                    message: format!("SslConnector::builder failed: {e}"),
                }
            })?;
            builder
                .set_ca_file(tls.ca_file.clone())
                .map_err(|e| BuildError::TlsBuild {
                    message: format!("set_ca_file({:?}) failed: {e}", tls.ca_file),
                })?;
            // Always require a valid peer certificate chain; this matches
            // both `SslMode::VerifyCa` and `SslMode::VerifyFull` on the
            // replication connection.
            builder.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
            let mut connector = MakeTlsConnector::new(builder.build());
            if !tls.verify_hostname {
                // verify-ca mode: validate the chain but skip the hostname
                // check, matching `SslMode::VerifyCa` on the replication
                // connection.
                connector.set_callback(|cfg, _domain| {
                    cfg.set_verify_hostname(false);
                    Ok(())
                });
            }
            let (client, connection) = pg_config
                .connect(connector)
                .await
                .map_err(|source| BuildError::SetupConnect { source })?;
            let handle = tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::warn!(
                        message = "PostgreSQL setup connection ended with error",
                        error = %e,
                    );
                }
            });
            (client, handle)
        }
        None => {
            let (client, connection) = pg_config
                .connect(NoTls)
                .await
                .map_err(|source| BuildError::SetupConnect { source })?;
            let handle = tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::warn!(
                        message = "PostgreSQL setup connection ended with error",
                        error = %e,
                    );
                }
            });
            (client, handle)
        }
    };

    // `pg_replication_slots.wal_status` was added in PostgreSQL 13. We
    // support older servers (PG 10+, the minimum that supports logical
    // replication) by issuing a column-free query when the server is too
    // old. This matches `postgresql_metrics`, which performs similar
    // runtime version detection rather than hard-requiring a specific
    // major version. The `SlotWalLost` diagnostic is unavailable on PG 12
    // and earlier; on those versions, monitor slot lag out-of-band (e.g.
    // via the `postgresql_metrics` source).
    let server_version: i32 = client
        .query_one("SHOW server_version_num", &[])
        .await
        .map_err(|source| BuildError::QueryFailed { source })?
        .get::<_, String>(0)
        .parse()
        .unwrap_or(0);
    let has_wal_status = server_version >= 130_000;

    let row = if has_wal_status {
        client
            .query_opt(
                "SELECT plugin, confirmed_flush_lsn::text, active, \
                     COALESCE(wal_status, 'unknown') AS wal_status \
                 FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
    } else {
        client
            .query_opt(
                "SELECT plugin, confirmed_flush_lsn::text, active \
                 FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
    }
    .map_err(|source| BuildError::QueryFailed { source })?;

    let result = match row {
        None => Err(BuildError::SlotMissing {
            slot: slot.to_owned(),
        }),
        Some(row) => {
            let plugin: String = row.get(0);
            let active: bool = row.get(2);
            let wal_status: String = if has_wal_status {
                row.get(3)
            } else {
                "unknown".to_owned()
            };
            if plugin != "pgoutput" {
                Err(BuildError::WrongOutputPlugin {
                    slot: slot.to_owned(),
                    plugin,
                })
            } else if active {
                Err(BuildError::SlotAlreadyActive {
                    slot: slot.to_owned(),
                })
            } else if wal_status == "lost" {
                Err(BuildError::SlotWalLost {
                    slot: slot.to_owned(),
                    status: wal_status,
                })
            } else {
                let confirmed: Option<String> = row.get(1);
                tracing::info!(
                    message = "Attaching to PostgreSQL replication slot.",
                    slot = slot,
                    server_version_num = server_version,
                    confirmed_flush_lsn = confirmed.as_deref().unwrap_or("none"),
                    wal_status = %wal_status,
                );
                Ok(())
            }
        }
    };

    // Close the client so the connection task can exit. Awaiting the
    // background task ensures any final tracing it emits surfaces before
    // we move on (and surfaces panics as JoinError on the next line).
    drop(client);
    if let Err(e) = connection_handle.await
        && e.is_panic()
    {
        tracing::error!(
            message = "Setup connection task panicked",
            error = %e,
        );
    }
    result
}

/// Builds the `pgwire-replication` config from the user's libpq URI plus the
/// CDC-specific knobs.
///
/// ## Safety contract
///
/// The returned `PgwireConfig` carries the password in a `String`. The type
/// derives `Debug` upstream and the password is not redacted. **Callers must
/// not** format the returned value with `{:?}`, `?cfg`, or any tracing macro
/// that would render it. Only individual safe fields may be logged.
fn build_replication_config(
    pg_config: &tokio_postgres::Config,
    cdc_config: &PostgresqlCdcConfig,
) -> Result<PgwireConfig, BuildError> {
    let user = pg_config
        .get_user()
        .ok_or(BuildError::MissingConnectionField { field: "user" })?;
    let password_bytes = pg_config
        .get_password()
        .ok_or(BuildError::MissingConnectionField { field: "password" })?;
    // Use strict UTF-8 decoding. `from_utf8_lossy` would silently substitute
    // U+FFFD for non-UTF-8 bytes, producing a credential that does not match
    // the user's actual password and burying the root cause in a confusing
    // auth-failed error.
    let password = std::str::from_utf8(password_bytes)
        .map_err(|_| BuildError::NonUtf8Password)?
        .to_owned();
    let database = pg_config
        .get_dbname()
        .ok_or(BuildError::MissingConnectionField { field: "dbname" })?;
    let host = first_host(pg_config).ok_or(BuildError::MissingConnectionField { field: "host" })?;
    let port = pg_config.get_ports().first().copied().unwrap_or(5432);

    let start_lsn = match cdc_config.slot_start_lsn.as_deref() {
        Some(s) => Lsn::parse(s).map_err(|_| BuildError::InvalidStartLsn { lsn: s.to_owned() })?,
        None => Lsn::ZERO,
    };

    let tls = match &cdc_config.tls {
        None => PgwireTlsConfig::disabled(),
        Some(tls) => {
            let mode = if tls.verify_hostname {
                SslMode::VerifyFull
            } else {
                SslMode::VerifyCa
            };
            PgwireTlsConfig {
                mode,
                ca_pem_path: Some(tls.ca_file.clone()),
                ..Default::default()
            }
        }
    };

    Ok(PgwireConfig {
        host: host.to_owned(),
        port,
        user: user.to_owned(),
        password,
        database: database.to_owned(),
        tls,
        slot: cdc_config.replication_slot.clone(),
        publication: cdc_config.publication_name.clone(),
        start_lsn,
        status_interval: STATUS_UPDATE_INTERVAL,
        ..PgwireConfig::default()
    })
}

fn first_host(pg_config: &tokio_postgres::Config) -> Option<String> {
    pg_config.get_hosts().first().and_then(|h| match h {
        tokio_postgres::config::Host::Tcp(s) => Some(s.clone()),
        #[cfg(unix)]
        tokio_postgres::config::Host::Unix(p) => p.to_str().map(str::to_owned),
        #[cfg(not(unix))]
        _ => None,
    })
}

async fn stream_events(
    replication_config: PgwireConfig,
    mut out: SourceSender,
    mut shutdown: ShutdownSignal,
    acknowledgements: bool,
    log_namespace: LogNamespace,
) -> Result<(), ()> {
    let mut client = match ReplicationClient::connect(replication_config).await {
        Ok(c) => c,
        Err(error) => {
            emit!(PostgresqlCdcReplicationError {
                error: error.to_string(),
            });
            return Err(());
        }
    };

    let mut parser = PgoutputParser::new();
    let mut tracker = LsnTracker::new();
    let mut tx_meta: Option<TxMeta> = None;
    let mut tx_receivers: Vec<BatchStatusReceiver> = Vec::new();
    let mut last_advanced = 0u64;
    // Edge-triggered tracker for the back-pressure warning. We only want to
    // log once when the tracker fills and once when it drains, not on every
    // loop iteration while a slow sink keeps us saturated.
    let mut tracker_was_full = false;
    let bytes_received = register!(BytesReceived::from(Protocol::TCP));
    let events_received = register!(EventsReceived);

    loop {
        // Poll the tracker first so confirmed LSNs are forwarded promptly.
        if let Some(new_lsn) = tracker.poll_confirmed()
            && new_lsn != last_advanced
        {
            client.update_applied_lsn(Lsn::from_u64(new_lsn));
            last_advanced = new_lsn;
        }
        if let Some((lsn, status)) = tracker.failure() {
            emit!(PostgresqlCdcSinkAckFailed {
                lsn,
                status: batch_status_name(status),
            });
            client.stop();
            return Err(());
        }

        // Back-pressure: when the LSN tracker is at its cap, stop pulling
        // new events off the wire. The select! arm guard below conditionally
        // skips `client.recv()`, so the only futures driving the select are
        // shutdown and the periodic tracker poll, letting downstream acks
        // drain the tracker before we accept more work. This keeps a slow
        // sink from OOMing the source by accumulating unbounded LSN state.
        let tracker_is_full = tracker.is_full();
        if tracker_is_full && !tracker_was_full {
            tracing::warn!(
                message = "LSN tracker at capacity; pausing replication intake until acks drain.",
            );
        } else if !tracker_is_full && tracker_was_full {
            tracing::info!(
                message = "LSN tracker drained below capacity; resuming replication intake.",
            );
        }
        tracker_was_full = tracker_is_full;

        tokio::select! {
            biased;

            _ = &mut shutdown => {
                tracing::info!(message = "Shutdown requested; stopping replication stream.");
                client.stop();
                return Ok(());
            }

            event = client.recv(), if !tracker.is_full() => match event {
                Ok(Some(event)) => {
                    if let Err(()) = handle_event(
                        event,
                        &mut parser,
                        &mut tracker,
                        &mut tx_meta,
                        &mut tx_receivers,
                        &mut out,
                        acknowledgements,
                        log_namespace,
                        &bytes_received,
                        &events_received,
                    ).await {
                        return Err(());
                    }
                }
                Ok(None) => {
                    tracing::info!(message = "Replication stream ended.");
                    return Ok(());
                }
                Err(error) => {
                    emit!(PostgresqlCdcReplicationError {
                        error: error.to_string(),
                    });
                    return Err(());
                }
            },

            _ = sleep(TRACKER_POLL_INTERVAL) => {
                // Wake up to poll the tracker. The next loop iteration handles
                // forwarding any confirmed LSN to the replication client.
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_event(
    event: ReplicationEvent,
    parser: &mut PgoutputParser,
    tracker: &mut LsnTracker,
    tx_meta: &mut Option<TxMeta>,
    tx_receivers: &mut Vec<BatchStatusReceiver>,
    out: &mut SourceSender,
    acknowledgements: bool,
    log_namespace: LogNamespace,
    bytes_received: &Registered<BytesReceived>,
    events_received: &Registered<EventsReceived>,
) -> Result<(), ()> {
    match event {
        ReplicationEvent::Begin {
            final_lsn,
            xid,
            commit_time_micros,
        } => {
            *tx_meta = Some(TxMeta {
                final_lsn: final_lsn.as_u64(),
                transaction_id: xid,
                commit_timestamp: pg_micros_to_utc(commit_time_micros),
            });
            Ok(())
        }
        ReplicationEvent::Commit { .. } => {
            if let Some(tx) = tx_meta.take() {
                for rx in tx_receivers.drain(..) {
                    tracker.track(tx.final_lsn, rx);
                }
                if !acknowledgements {
                    tracker.mark_delivered_immediately(tx.final_lsn);
                }
            }
            Ok(())
        }
        ReplicationEvent::XLogData { data, .. } => {
            bytes_received.emit(vector_lib::internal_event::ByteSize(data.len()));
            let tx_snapshot = *tx_meta;
            match parser.parse(&data, tx_snapshot.as_ref(), log_namespace) {
                Ok(Parsed::NoEvent) => Ok(()),
                Ok(Parsed::Rows(rows)) => {
                    if tx_snapshot.is_none() {
                        emit!(PostgresqlCdcProtocolViolation {
                            detail: "row message outside any transaction",
                        });
                        return Err(());
                    }
                    let logs: Vec<LogEvent> = rows.into_iter().map(|r| r.log).collect();
                    if let Some(rx) =
                        send_log_events(logs, out, acknowledgements, events_received).await?
                    {
                        tx_receivers.push(rx);
                    }
                    Ok(())
                }
                Err(error) => {
                    emit!(PostgresqlCdcParseError {
                        error: error.to_string(),
                    });
                    Err(())
                }
            }
        }
        ReplicationEvent::KeepAlive { .. } => Ok(()),
        ReplicationEvent::Message {
            transactional,
            lsn,
            prefix,
            content,
        } => {
            bytes_received.emit(vector_lib::internal_event::ByteSize(content.len()));
            let (track_lsn, log) = if transactional {
                let Some(tx) = tx_meta.as_ref() else {
                    emit!(PostgresqlCdcProtocolViolation {
                        detail: "transactional logical message outside any transaction",
                    });
                    return Err(());
                };
                (
                    tx.final_lsn,
                    build_message_event(
                        true,
                        lsn.as_u64(),
                        &prefix,
                        &content,
                        Some(tx),
                        log_namespace,
                    ),
                )
            } else {
                (
                    lsn.as_u64(),
                    build_message_event(
                        false,
                        lsn.as_u64(),
                        &prefix,
                        &content,
                        None,
                        log_namespace,
                    ),
                )
            };
            let rx = send_log_events(vec![log], out, acknowledgements, events_received).await?;
            if transactional {
                if let Some(rx) = rx {
                    tx_receivers.push(rx);
                }
            } else if let Some(rx) = rx {
                tracker.track(track_lsn, rx);
            } else {
                tracker.mark_delivered_immediately(track_lsn);
            }
            Ok(())
        }
        ReplicationEvent::StoppedAt { reached } => {
            tracing::info!(message = "Replication reached stop LSN", lsn = %reached);
            Ok(())
        }
    }
}

const fn batch_status_name(status: vector_lib::event::BatchStatus) -> &'static str {
    match status {
        vector_lib::event::BatchStatus::Delivered => "delivered",
        vector_lib::event::BatchStatus::Errored => "errored",
        vector_lib::event::BatchStatus::Rejected => "rejected",
    }
}

async fn send_log_events(
    logs: Vec<LogEvent>,
    out: &mut SourceSender,
    acknowledgements: bool,
    events_received: &Registered<EventsReceived>,
) -> Result<Option<BatchStatusReceiver>, ()> {
    let count = logs.len();
    let byte_size = logs.estimated_json_encoded_size_of();
    events_received.emit(CountByteSize(count, byte_size));

    let (events, receiver) = if acknowledgements {
        let (batch, receiver) = BatchNotifier::new_with_receiver();
        let events: Vec<Event> = logs
            .into_iter()
            .map(|log| {
                let event: Event = log.into();
                event.with_batch_notifier(&batch)
            })
            .collect();
        drop(batch);
        (events, Some(receiver))
    } else {
        (logs.into_iter().map(Into::into).collect(), None)
    };

    match out.send_batch(events).await {
        Ok(()) => Ok(receiver),
        Err(_error) => {
            emit!(StreamClosedError { count });
            Err(())
        }
    }
}

fn build_message_event(
    transactional: bool,
    lsn: u64,
    prefix: &str,
    content: &bytes::Bytes,
    tx_meta: Option<&TxMeta>,
    log_namespace: LogNamespace,
) -> LogEvent {
    use crate::sources::postgresql_cdc::config::PostgresqlCdcConfig;
    use vector_lib::config::LegacyKey;
    use vrl::path;

    let mut log = LogEvent::default();
    log_namespace.insert_source_metadata(
        PostgresqlCdcConfig::NAME,
        &mut log,
        Some(LegacyKey::Overwrite(path!("operation"))),
        path!("operation"),
        "message",
    );
    log_namespace.insert_source_metadata(
        PostgresqlCdcConfig::NAME,
        &mut log,
        Some(LegacyKey::Overwrite(path!("transactional"))),
        path!("transactional"),
        Value::Boolean(transactional),
    );
    log_namespace.insert_source_metadata(
        PostgresqlCdcConfig::NAME,
        &mut log,
        Some(LegacyKey::Overwrite(path!("lsn"))),
        path!("lsn"),
        format_lsn(lsn),
    );
    log_namespace.insert_source_metadata(
        PostgresqlCdcConfig::NAME,
        &mut log,
        Some(LegacyKey::Overwrite(path!("prefix"))),
        path!("prefix"),
        prefix.to_owned(),
    );
    log_namespace.insert_source_metadata(
        PostgresqlCdcConfig::NAME,
        &mut log,
        Some(LegacyKey::Overwrite(path!("content"))),
        path!("content"),
        Value::Bytes(content.clone()),
    );
    if let Some(tx) = tx_meta {
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
    }
    log_namespace.insert_standard_vector_source_metadata(
        &mut log,
        PostgresqlCdcConfig::NAME,
        Utc::now(),
    );
    // `new`/`old`/`schema`/`table`/`transaction_id`/`transaction_timestamp`
    // are deliberately absent on `message` events (the latter two are only
    // present for transactional messages, above). Consumers should
    // pattern-match by `operation == "message"` and key-absence rather
    // than checking for `null`, so we do not insert sentinel nulls.
    log
}

fn pg_micros_to_utc(micros: i64) -> chrono::DateTime<Utc> {
    let unix_micros = micros.saturating_add(PG_EPOCH_OFFSET_MICROS);
    Utc.timestamp_micros(unix_micros)
        .single()
        .unwrap_or_else(|| {
            // Out-of-range or ambiguous PG timestamp. This should not occur in
            // practice — `pg_micros_to_utc(0)` covers the maximum offset and
            // `i64` micros covers the chrono range — but if the server emits a
            // value chrono cannot resolve, log it so the synthetic Unix-epoch
            // fallback below is visible to operators rather than silently
            // tagging events with `1970-01-01`.
            tracing::warn!(
                message = "PostgreSQL commit timestamp out of representable range; \
                       falling back to Unix epoch.",
                pg_micros = micros,
            );
            Utc.timestamp_opt(0, 0)
                .single()
                .expect("Unix epoch is always representable as DateTime<Utc>")
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use vector_lib::sensitive_string::SensitiveString;

    fn base_config() -> PostgresqlCdcConfig {
        PostgresqlCdcConfig {
            connection_string: SensitiveString::from("postgres://u:p@127.0.0.1:5432/db".to_owned()),
            replication_slot: "slot".to_owned(),
            publication_name: "pub".to_owned(),
            slot_start_lsn: None,
            tls: None,
            acknowledgements: Default::default(),
            log_namespace: None,
        }
    }

    fn parse_pg(uri: &str) -> tokio_postgres::Config {
        uri.parse().expect("valid libpq URI")
    }

    #[test]
    fn pg_epoch_translates_to_unix() {
        // PostgreSQL epoch == 2000-01-01 00:00:00 UTC, which is 946684800
        // seconds (946684800_000_000 microseconds) past Unix epoch.
        let pg_zero = pg_micros_to_utc(0);
        assert_eq!(pg_zero.timestamp(), 946_684_800);
    }

    #[test]
    fn invalid_start_lsn_surfaces_typed_error() {
        let mut cfg = base_config();
        cfg.slot_start_lsn = Some("not-an-lsn".to_owned());
        let pg = parse_pg("postgres://u:p@127.0.0.1:5432/db");
        let err = build_replication_config(&pg, &cfg).expect_err("must reject malformed LSN");
        assert!(
            matches!(err, BuildError::InvalidStartLsn { ref lsn } if lsn == "not-an-lsn"),
            "expected InvalidStartLsn, got {err:?}"
        );
    }

    #[test]
    fn missing_password_surfaces_typed_error() {
        let cfg = base_config();
        // libpq URI with user but no password.
        let pg = parse_pg("postgres://u@127.0.0.1:5432/db");
        let err = build_replication_config(&pg, &cfg).expect_err("must reject missing password");
        assert!(
            matches!(
                err,
                BuildError::MissingConnectionField { field: "password" }
            ),
            "expected MissingConnectionField{{password}}, got {err:?}"
        );
    }

    #[test]
    fn missing_dbname_surfaces_typed_error() {
        let cfg = base_config();
        // libpq URI with user/password but no dbname.
        let pg = parse_pg("postgres://u:p@127.0.0.1:5432");
        let err = build_replication_config(&pg, &cfg).expect_err("must reject missing dbname");
        assert!(
            matches!(err, BuildError::MissingConnectionField { field: "dbname" }),
            "expected MissingConnectionField{{dbname}}, got {err:?}"
        );
    }

    #[test]
    fn non_utf8_password_surfaces_typed_error() {
        let cfg = base_config();
        // Construct a tokio_postgres::Config with a non-UTF-8 password byte
        // sequence. The libpq URI parser percent-decodes, so we set the
        // password directly on the builder to keep this purely about the
        // UTF-8 check inside `build_replication_config`.
        let mut pg = parse_pg("postgres://u@127.0.0.1:5432/db");
        pg.password([0xff, 0xfe, 0xfd]);
        let err = build_replication_config(&pg, &cfg).expect_err("must reject non-UTF-8 password");
        assert!(
            matches!(err, BuildError::NonUtf8Password),
            "expected NonUtf8Password, got {err:?}"
        );
    }
}
