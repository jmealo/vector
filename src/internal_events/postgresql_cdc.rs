use vector_lib::{
    NamedInternalEvent, counter,
    internal_event::{CounterName, InternalEvent, error_stage, error_type},
};

/// Emitted when the pgoutput parser fails to decode an XLogData payload from
/// the server. Halts the source — log + counter + error_code tag so operators
/// can correlate to PG-side issues (corrupt WAL, protocol-version skew, etc).
#[derive(Debug, NamedInternalEvent)]
pub struct PostgresqlCdcParseError {
    pub error: String,
}

impl InternalEvent for PostgresqlCdcParseError {
    fn emit(self) {
        error!(
            message = "Failed to parse pgoutput message; halting source to avoid data loss.",
            error = %self.error,
            error_code = "pgoutput_parse",
            error_type = error_type::PARSER_FAILED,
            stage = error_stage::PROCESSING,
        );
        counter!(
            CounterName::ComponentErrorsTotal,
            "error_code" => "pgoutput_parse",
            "error_type" => error_type::PARSER_FAILED,
            "stage" => error_stage::PROCESSING,
        )
        .increment(1);
    }
}

/// Emitted when the replication stream itself reports an error
/// (connection drop, server-side error, auth failure, etc).
#[derive(Debug, NamedInternalEvent)]
pub struct PostgresqlCdcReplicationError {
    pub error: String,
}

impl InternalEvent for PostgresqlCdcReplicationError {
    fn emit(self) {
        error!(
            message = "Replication stream error.",
            error = %self.error,
            error_code = "replication_stream",
            error_type = error_type::READER_FAILED,
            stage = error_stage::RECEIVING,
        );
        counter!(
            CounterName::ComponentErrorsTotal,
            "error_code" => "replication_stream",
            "error_type" => error_type::READER_FAILED,
            "stage" => error_stage::RECEIVING,
        )
        .increment(1);
    }
}

/// Emitted when a sink reports `Errored` or `Rejected` for an LSN's batch
/// of events. The source halts; topology will restart it from the slot's
/// last confirmed LSN, so the affected transaction will be re-delivered.
#[derive(Debug, NamedInternalEvent)]
pub struct PostgresqlCdcSinkAckFailed {
    pub lsn: u64,
    pub status: &'static str,
}

impl InternalEvent for PostgresqlCdcSinkAckFailed {
    fn emit(self) {
        error!(
            message = "Sink reported failed delivery; stopping replication.",
            lsn = %self.lsn,
            status = self.status,
            error_code = "sink_ack_failed",
            error_type = error_type::ACKNOWLEDGMENT_FAILED,
            stage = error_stage::PROCESSING,
        );
        counter!(
            CounterName::ComponentErrorsTotal,
            "error_code" => "sink_ack_failed",
            "error_type" => error_type::ACKNOWLEDGMENT_FAILED,
            "stage" => error_stage::PROCESSING,
        )
        .increment(1);
    }
}

/// Emitted on a protocol-state violation (a row event arrives outside a
/// transaction, or a transactional logical message arrives outside a txn).
/// Halts the source. Should never fire in practice; if it does, it indicates
/// either a Postgres bug or a pgwire-replication regression.
#[derive(Debug, NamedInternalEvent)]
pub struct PostgresqlCdcProtocolViolation {
    pub detail: &'static str,
}

impl InternalEvent for PostgresqlCdcProtocolViolation {
    fn emit(self) {
        error!(
            message = "pgoutput protocol violation; halting source.",
            detail = self.detail,
            error_code = "protocol_violation",
            error_type = error_type::CONDITION_FAILED,
            stage = error_stage::PROCESSING,
        );
        counter!(
            CounterName::ComponentErrorsTotal,
            "error_code" => "protocol_violation",
            "error_type" => error_type::CONDITION_FAILED,
            "stage" => error_stage::PROCESSING,
        )
        .increment(1);
    }
}
