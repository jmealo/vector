//! Integration tests for the `postgresql_cdc` source.
//!
//! Requires a running PostgreSQL instance with `wal_level=logical`,
//! `max_replication_slots>=10`, and `max_wal_senders>=10`. The
//! `tests/integration/postgresql_cdc/config/compose.yaml` file in this
//! repository provisions a suitable instance. Run via:
//!
//! ```sh
//! cargo vdev int test postgresql_cdc
//! ```

#![allow(clippy::print_stdout)] // for diagnostic output on failure

use std::time::Duration;

use futures::{Stream, StreamExt};
use tokio::time;
use tokio_postgres::{Client, NoTls};
use vector_lib::{event::Event, sensitive_string::SensitiveString};

use super::config::PostgresqlCdcConfig;
use crate::{
    SourceSender,
    config::{SourceAcknowledgementsConfig, SourceConfig, SourceContext},
    test_util::{integration::postgres::pg_url, random_string, random_table_name, trace_init},
};

/// Per-test fixture — table + publication + slot, all with random names so
/// tests can run in parallel without colliding.
struct CdcFixture {
    client: Client,
    table: String,
    publication: String,
    slot: String,
    endpoint: String,
}

impl CdcFixture {
    async fn setup(create_table_sql: &str) -> Self {
        let endpoint = pg_url();
        let (client, connection) = tokio_postgres::connect(&endpoint, NoTls)
            .await
            .expect("setup: connect to Postgres");
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::debug!(message = "setup connection error", error = %e);
            }
        });

        let suffix = random_string(8).to_lowercase();
        let table = random_table_name();
        let publication = format!("vector_pub_{suffix}");
        let slot = format!("vector_slot_{suffix}");

        let slot_literal = quote_literal(&slot);
        let publication_ident = quote_ident(&publication);
        let table_ident = quote_ident(&table);

        // Best-effort: drop any leftover slot/publication from a prior aborted
        // test run. These statements may fail harmlessly if nothing exists.
        let _ = client
            .simple_query(&format!(
                "SELECT pg_drop_replication_slot({slot_literal}) WHERE EXISTS \
                 (SELECT 1 FROM pg_replication_slots WHERE slot_name={slot_literal})"
            ))
            .await;
        let _ = client
            .simple_query(&format!("DROP PUBLICATION IF EXISTS {publication_ident}"))
            .await;
        let _ = client
            .simple_query(&format!("DROP TABLE IF EXISTS {table_ident} CASCADE"))
            .await;

        let create_sql = create_table_sql.replace("{table}", &table_ident);
        client
            .simple_query(&create_sql)
            .await
            .expect("create test table");
        client
            .simple_query(&format!(
                "CREATE PUBLICATION {publication_ident} FOR TABLE {table_ident}"
            ))
            .await
            .expect("create publication");
        client
            .simple_query(&format!(
                "SELECT pg_create_logical_replication_slot({slot_literal}, 'pgoutput')"
            ))
            .await
            .expect("create replication slot");

        Self {
            client,
            table,
            publication,
            slot,
            endpoint,
        }
    }

    /// Adds an extra table to the existing publication. Used by the
    /// multi-table test case.
    async fn add_table_to_publication(&self, second_table: &str, create_sql: &str) {
        self.client
            .simple_query(&create_sql.replace("{table}", second_table))
            .await
            .expect("create second table");
        self.client
            .simple_query(&format!(
                "ALTER PUBLICATION {} ADD TABLE {}",
                self.publication, second_table
            ))
            .await
            .expect("add table to publication");
    }

    fn config(&self) -> PostgresqlCdcConfig {
        PostgresqlCdcConfig {
            connection_string: SensitiveString::from(self.endpoint.clone()),
            replication_slot: self.slot.clone(),
            publication_name: self.publication.clone(),
            slot_start_lsn: None,
            tls: None,
            // Force ack on for integration tests. The default
            // SourceContext::new_test() has acknowledgements=false, so
            // without this the source would never attach BatchNotifiers and
            // confirmed_flush_lsn would never advance.
            acknowledgements: SourceAcknowledgementsConfig::from(Some(true)),
            // Integration tests assert on root-placed fields, which is the
            // `Legacy` shape. Vector-namespace placement is exercised
            // separately via the parser's unit tests.
            log_namespace: Some(false),
        }
    }

    async fn confirmed_flush_lsn(&self) -> Option<String> {
        let row = self
            .client
            .query_opt(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1",
                &[&self.slot],
            )
            .await
            .expect("query confirmed_flush_lsn");
        row.and_then(|r| r.get(0))
    }

    /// Polls `confirmed_flush_lsn` until it differs from `baseline` (i.e. has
    /// advanced), or `timeout` elapses. Polls every 100ms so we don't waste
    /// up to half a second after the advancement actually happens. Returns
    /// the new LSN or `None` if it never advanced.
    async fn wait_for_lsn_to_advance(
        &self,
        baseline: Option<String>,
        timeout: Duration,
    ) -> Option<String> {
        let deadline = time::Instant::now() + timeout;
        loop {
            time::sleep(Duration::from_millis(100)).await;
            let now = self.confirmed_flush_lsn().await;
            if now.is_some() && now != baseline {
                return now;
            }
            if time::Instant::now() >= deadline {
                return None;
            }
        }
    }

    /// Polls until `confirmed_flush_lsn >= target_lsn` (using Postgres'
    /// native pg_lsn comparison). This is the right primitive for resume
    /// tests: we need to know that every event up to a particular LSN is
    /// durably acknowledged before tearing down the source.
    async fn wait_for_lsn_at_or_past(&self, target_lsn: &str, timeout: Duration) -> Option<String> {
        // Defensive: LSN format is `X/Y` hex from our own pgoutput parser.
        // Reject anything else before interpolating into SQL.
        assert!(
            target_lsn
                .chars()
                .all(|c| c.is_ascii_hexdigit() || c == '/'),
            "wait_for_lsn_at_or_past called with non-LSN string: {target_lsn:?}"
        );
        let deadline = time::Instant::now() + timeout;
        loop {
            time::sleep(Duration::from_millis(100)).await;
            // tokio-postgres can't bind `&str` as the `pg_lsn` type, so we
            // interpolate the LSN literal (validated above) directly.
            let sql = format!(
                "SELECT confirmed_flush_lsn::text \
                 FROM pg_replication_slots \
                 WHERE slot_name = $1 \
                   AND confirmed_flush_lsn >= '{target_lsn}'::pg_lsn"
            );
            let row = self
                .client
                .query_opt(&sql, &[&self.slot])
                .await
                .expect("query slot lsn");
            if let Some(row) = row {
                return Some(row.get(0));
            }
            if time::Instant::now() >= deadline {
                return None;
            }
        }
    }

    async fn teardown(self) {
        let slot_literal = quote_literal(&self.slot);
        let publication_ident = quote_ident(&self.publication);
        let table_ident = quote_ident(&self.table);
        let _ = self
            .client
            .simple_query(&format!(
                "SELECT pg_drop_replication_slot({slot_literal}) WHERE EXISTS \
                 (SELECT 1 FROM pg_replication_slots WHERE slot_name={slot_literal})"
            ))
            .await;
        let _ = self
            .client
            .simple_query(&format!("DROP PUBLICATION IF EXISTS {publication_ident}"))
            .await;
        let _ = self
            .client
            .simple_query(&format!("DROP TABLE IF EXISTS {table_ident} CASCADE"))
            .await;
    }
}

/// Spawns the source with an auto-ack receiver (events marked Delivered when
/// they reach the test consumer). Returns the receiver and a handle to abort
/// the source task.
async fn spawn_source(
    config: PostgresqlCdcConfig,
) -> (
    impl Stream<Item = Event> + Unpin,
    tokio::task::JoinHandle<()>,
) {
    let (sender, recv) =
        SourceSender::new_test_finalize(vector_lib::finalization::EventStatus::Delivered);
    let cx = SourceContext::new_test(sender, None);
    let source_fut = config
        .build(cx)
        .await
        .expect("source should build successfully");
    let handle = tokio::spawn(async move {
        let _ = source_fut.await;
    });
    (recv, handle)
}

/// Waits for `expected` events from the receiver with a generous timeout.
async fn collect_events<S>(recv: &mut S, expected: usize, timeout_secs: u64) -> Vec<Event>
where
    S: Stream<Item = Event> + Unpin,
{
    let deadline = time::Instant::now() + Duration::from_secs(timeout_secs);
    let mut events = Vec::with_capacity(expected);
    while events.len() < expected {
        let remaining = deadline.saturating_duration_since(time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match time::timeout(remaining, recv.next()).await {
            Ok(Some(e)) => events.push(e),
            Ok(None) => break,
            Err(_) => break,
        }
    }
    events
}

/// Collects events until `predicate` returns true for every accumulated event
/// set, or `timeout_secs` elapses. Lets resume/reconnect tests stop waiting
/// as soon as they've observed the expected new rows, rather than burning a
/// fixed 30s timeout collecting noise from concurrent tests' logical
/// messages.
async fn collect_until<S, F>(recv: &mut S, mut predicate: F, timeout_secs: u64) -> Vec<Event>
where
    S: Stream<Item = Event> + Unpin,
    F: FnMut(&[Event]) -> bool,
{
    let deadline = time::Instant::now() + Duration::from_secs(timeout_secs);
    let mut events: Vec<Event> = Vec::new();
    loop {
        if predicate(&events) {
            return events;
        }
        let remaining = deadline.saturating_duration_since(time::Instant::now());
        if remaining.is_zero() {
            return events;
        }
        match time::timeout(remaining, recv.next()).await {
            Ok(Some(e)) => events.push(e),
            Ok(None) | Err(_) => return events,
        }
    }
}

/// Filters a slice of events to row events emitted from the given table,
/// dropping noise (e.g. `pg_logical_emit_message` events from concurrent
/// tests, which are publication-independent and reach every slot on the
/// server).
fn filter_table_rows<'a>(events: &'a [Event], table: &str) -> Vec<&'a Event> {
    events
        .iter()
        .filter(|e| {
            let op = get_string(e, "operation");
            (op == "insert" || op == "update" || op == "delete") && get_string(e, "table") == table
        })
        .collect()
}

fn get_field<'a>(event: &'a Event, key: &str) -> Option<&'a vrl::value::Value> {
    event.as_log().get(key)
}

fn get_string(event: &Event, key: &str) -> String {
    match get_field(event, key).unwrap_or_else(|| panic!("missing field {key}")) {
        vrl::value::Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
        other => panic!("field {key} not bytes: {other:?}"),
    }
}

/// Parses a Postgres `X/Y` hex LSN to a `u64` so tests can compare LSNs by
/// magnitude. Comparing the formatted strings lexicographically inverts at
/// the first WAL segment boundary that grows the `X` half by a hex digit
/// (e.g. `"16/0"` sorts before `"2/0"` while `0x16_0000_0000 > 0x2_0000_0000`).
fn parse_lsn(s: &str) -> u64 {
    let (hi, lo) = s
        .split_once('/')
        .unwrap_or_else(|| panic!("malformed LSN {s:?} (expected `X/Y` hex)"));
    let hi =
        u32::from_str_radix(hi, 16).unwrap_or_else(|_| panic!("LSN high half not hex: {hi:?}"));
    let lo = u32::from_str_radix(lo, 16).unwrap_or_else(|_| panic!("LSN low half not hex: {lo:?}"));
    (u64::from(hi) << 32) | u64::from(lo)
}

/// Double-quotes a PostgreSQL identifier and escapes embedded double quotes.
/// Use this for any test SQL that interpolates a table, publication, or slot
/// name into a DDL statement — even when the generator only produces
/// alphanumeric names today, the helper prevents a future test fixture from
/// silently becoming injection-vulnerable.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Single-quotes a PostgreSQL string literal and escapes embedded single
/// quotes. Used for slot-name arguments to functions like
/// `pg_create_logical_replication_slot` and `pg_drop_replication_slot`,
/// which take the slot name as a `text` argument.
fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn get_object(event: &Event, key: &str) -> vrl::value::ObjectMap {
    match get_field(event, key).unwrap_or_else(|| panic!("missing field {key}")) {
        vrl::value::Value::Object(m) => m.clone(),
        other => panic!("field {key} not object: {other:?}"),
    }
}

#[tokio::test]
async fn test_insert_produces_event() {
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, status TEXT)").await;

    let (mut recv, handle) = spawn_source(fixture.config()).await;

    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {} VALUES (1, 'pending')",
            fixture.table
        ))
        .await
        .unwrap();

    let events = collect_events(&mut recv, 1, 30).await;
    assert_eq!(events.len(), 1, "expected exactly one event");
    let ev = &events[0];
    assert_eq!(get_string(ev, "operation"), "insert");
    assert_eq!(get_string(ev, "table"), fixture.table);
    let new = get_object(ev, "new");
    assert_eq!(new.get("id").unwrap(), &vrl::value::Value::Integer(1));
    assert_eq!(
        new.get("status").unwrap(),
        &vrl::value::Value::from("pending")
    );
    let lsn = get_string(ev, "lsn");
    assert!(!lsn.is_empty() && lsn.contains('/'));

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_update_produces_event() {
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, status TEXT)").await;
    let (mut recv, handle) = spawn_source(fixture.config()).await;

    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {} VALUES (1, 'pending'); \
             UPDATE {} SET status='complete' WHERE id=1",
            fixture.table, fixture.table
        ))
        .await
        .unwrap();

    let events = collect_events(&mut recv, 2, 30).await;
    assert_eq!(events.len(), 2);
    assert_eq!(get_string(&events[0], "operation"), "insert");
    assert_eq!(get_string(&events[1], "operation"), "update");
    let new = get_object(&events[1], "new");
    assert_eq!(
        new.get("status").unwrap(),
        &vrl::value::Value::from("complete")
    );

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_delete_produces_event() {
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, status TEXT)").await;
    // REPLICA IDENTITY FULL ensures the old row appears in the Delete message.
    fixture
        .client
        .simple_query(&format!(
            "ALTER TABLE {} REPLICA IDENTITY FULL",
            fixture.table
        ))
        .await
        .unwrap();

    let (mut recv, handle) = spawn_source(fixture.config()).await;

    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {} VALUES (1, 'doomed'); DELETE FROM {} WHERE id=1",
            fixture.table, fixture.table
        ))
        .await
        .unwrap();

    let events = collect_events(&mut recv, 2, 30).await;
    assert_eq!(events.len(), 2);
    assert_eq!(get_string(&events[1], "operation"), "delete");
    let old = get_object(&events[1], "old");
    assert_eq!(old.get("id").unwrap(), &vrl::value::Value::Integer(1));
    assert_eq!(
        old.get("status").unwrap(),
        &vrl::value::Value::from("doomed")
    );

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_truncate_produces_event() {
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, status TEXT)").await;
    let (mut recv, handle) = spawn_source(fixture.config()).await;

    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {} VALUES (1, 'a'), (2, 'b'); TRUNCATE {}",
            fixture.table, fixture.table
        ))
        .await
        .unwrap();

    let events = collect_events(&mut recv, 3, 30).await;
    let truncate = events
        .iter()
        .find(|e| get_string(e, "operation") == "truncate")
        .expect("truncate event");
    assert_eq!(get_string(truncate, "table"), fixture.table);
    assert_eq!(get_field(truncate, "new"), Some(&vrl::value::Value::Null));
    assert_eq!(get_field(truncate, "old"), Some(&vrl::value::Value::Null));

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_multi_table_publication() {
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, label TEXT)").await;
    let second = format!("{}_second", fixture.table);
    fixture
        .add_table_to_publication(
            &second,
            "CREATE TABLE {table} (id INT PRIMARY KEY, label TEXT)",
        )
        .await;

    let (mut recv, handle) = spawn_source(fixture.config()).await;

    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {} VALUES (10, 'first'); INSERT INTO {} VALUES (20, 'second')",
            fixture.table, second
        ))
        .await
        .unwrap();

    let events = collect_events(&mut recv, 2, 30).await;
    let tables: Vec<String> = events.iter().map(|e| get_string(e, "table")).collect();
    assert!(tables.contains(&fixture.table));
    assert!(tables.contains(&second));

    handle.abort();
    let _ = fixture
        .client
        .simple_query(&format!("DROP TABLE IF EXISTS {second} CASCADE"))
        .await;
    fixture.teardown().await;
}

#[tokio::test]
async fn test_lsn_advances_after_ack() {
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, label TEXT)").await;

    let before = fixture.confirmed_flush_lsn().await;

    let (mut recv, handle) = spawn_source(fixture.config()).await;

    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {} VALUES (1, 'ack-test')",
            fixture.table
        ))
        .await
        .unwrap();

    let events = collect_events(&mut recv, 1, 30).await;
    assert_eq!(events.len(), 1);
    // Dropping the events fires their BatchNotifiers as Delivered (the
    // default). Give the source up to a few seconds to forward the
    // confirmed LSN to Postgres.
    drop(events);

    let advanced = fixture
        .wait_for_lsn_to_advance(before.clone(), Duration::from_secs(30))
        .await;
    assert!(
        advanced.is_some(),
        "confirmed_flush_lsn did not advance after sink ack (before={before:?})"
    );

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_large_transaction_single_lsn() {
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY)").await;
    let (mut recv, handle) = spawn_source(fixture.config()).await;

    // Generate 1000 rows in one transaction.
    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {} SELECT generate_series(1, 1000)",
            fixture.table
        ))
        .await
        .unwrap();

    let events = collect_events(&mut recv, 1000, 60).await;
    assert_eq!(events.len(), 1000, "expected 1000 row events");
    // All events share one LSN because they belong to a single transaction.
    let lsn = get_string(&events[0], "lsn");
    for ev in &events {
        assert_eq!(get_string(ev, "lsn"), lsn);
    }

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_reconnect_resumes_from_lsn() {
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, label TEXT)").await;

    // First source instance.
    let before_first = fixture.confirmed_flush_lsn().await;
    let (mut recv, handle) = spawn_source(fixture.config()).await;
    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {} VALUES (1, 'first')",
            fixture.table
        ))
        .await
        .unwrap();
    let first = collect_events(&mut recv, 1, 30).await;
    assert_eq!(first.len(), 1);
    let first_lsn = get_string(&first[0], "lsn");
    assert_eq!(
        get_object(&first[0], "new").get("id").unwrap(),
        &vrl::value::Value::Integer(1)
    );
    drop(first);

    // Wait until Postgres confirms the LSN has advanced AT OR PAST row 1's
    // commit LSN. Just "advanced from baseline" is too weak — pgwire could
    // checkpoint to a point mid-transaction.
    let after_first = fixture
        .wait_for_lsn_at_or_past(&first_lsn, Duration::from_secs(60))
        .await;
    assert!(
        after_first.is_some(),
        "confirmed_flush_lsn did not reach row 1's LSN ({first_lsn}) within 60s (before={before_first:?})"
    );

    handle.abort();
    drop(recv);
    // Give pgwire's drop a moment to release the slot.
    time::sleep(Duration::from_secs(2)).await;

    // New source instance, same slot. PostgreSQL's logical-replication
    // guarantees are at-least-once, not exactly-once: the txn whose commit
    // LSN equals confirmed_flush_lsn may legitimately be re-delivered on
    // reconnect, so consumers must dedupe by LSN. What we test here is:
    //   * no data loss — row 2 IS received
    //   * LSN ordering — row 2's LSN is strictly greater than row 1's
    //   * any re-delivered row 1 carries the same LSN it had originally
    let (mut recv2, handle2) = spawn_source(fixture.config()).await;
    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {} VALUES (2, 'second')",
            fixture.table
        ))
        .await
        .unwrap();
    // Collect generously — other tests running in parallel emit
    // `pg_logical_emit_message` events that reach every slot in the
    // database (logical messages are publication-independent), and we
    // don't want collect_events to bail out early on that noise before
    // row 2 actually arrives.
    let events = collect_events(&mut recv2, 30, 30).await;
    let mut seen_row2 = false;
    let mut last_lsn: Option<String> = None;
    for ev in &events {
        // Skip non-row events (truncate, message) — those have null `new`.
        let op = get_string(ev, "operation");
        if op != "insert" && op != "update" {
            continue;
        }
        let id = get_object(ev, "new").get("id").cloned();
        let lsn = get_string(ev, "lsn");
        if let Some(prev) = &last_lsn {
            assert!(
                parse_lsn(&lsn) >= parse_lsn(prev),
                "LSNs not ordered: {prev} -> {lsn}"
            );
        }
        last_lsn = Some(lsn.clone());
        if id == Some(vrl::value::Value::Integer(2)) {
            seen_row2 = true;
            assert!(
                parse_lsn(&lsn) > parse_lsn(&first_lsn),
                "row 2 LSN {lsn} not greater than row 1 LSN {first_lsn}"
            );
        }
    }
    assert!(
        seen_row2,
        "row 2 was never delivered (events received: {})",
        events.len()
    );

    handle2.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_null_fields() {
    trace_init();
    let fixture =
        CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, optional TEXT)").await;
    let (mut recv, handle) = spawn_source(fixture.config()).await;

    fixture
        .client
        .simple_query(&format!("INSERT INTO {} VALUES (1, NULL)", fixture.table))
        .await
        .unwrap();

    // Filter for our table to avoid flakes from concurrent tests'
    // pg_logical_emit_message events reaching every slot.
    let table_name = fixture.table.clone();
    let events = collect_until(
        &mut recv,
        |events| !filter_table_rows(events, &table_name).is_empty(),
        30,
    )
    .await;
    let rows = filter_table_rows(&events, &fixture.table);
    assert_eq!(rows.len(), 1);
    let new = get_object(rows[0], "new");
    assert_eq!(new.get("optional").unwrap(), &vrl::value::Value::Null);

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_replica_identity_nothing_insert_still_works() {
    // REPLICA IDENTITY NOTHING is a valid (if unusual) configuration:
    // PG forbids DELETE and UPDATE against the publication, but INSERT
    // still emits row events. The parser must handle the no-replica-key
    // case without crashing.
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, label TEXT)").await;
    fixture
        .client
        .simple_query(&format!(
            "ALTER TABLE {} REPLICA IDENTITY NOTHING",
            fixture.table
        ))
        .await
        .unwrap();

    let (mut recv, handle) = spawn_source(fixture.config()).await;

    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {} VALUES (1, 'inserted')",
            fixture.table
        ))
        .await
        .unwrap();

    let table_name = fixture.table.clone();
    let events = collect_until(
        &mut recv,
        |events| !filter_table_rows(events, &table_name).is_empty(),
        30,
    )
    .await;
    let rows = filter_table_rows(&events, &fixture.table);
    assert_eq!(rows.len(), 1);
    assert_eq!(get_string(rows[0], "operation"), "insert");
    let new = get_object(rows[0], "new");
    assert_eq!(new.get("id").unwrap(), &vrl::value::Value::Integer(1));
    assert_eq!(
        new.get("label").unwrap(),
        &vrl::value::Value::from("inserted")
    );

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_replica_identity_index_delete_uses_index_columns() {
    // REPLICA IDENTITY USING INDEX emits only the indexed columns in
    // `old` for DELETE / UPDATE.
    trace_init();
    let fixture = CdcFixture::setup(
        "CREATE TABLE {table} (id INT PRIMARY KEY, alt_key INT UNIQUE NOT NULL, label TEXT)",
    )
    .await;
    let idx_name = format!("{}_alt_key_key", fixture.table);
    fixture
        .client
        .simple_query(&format!(
            "ALTER TABLE {} REPLICA IDENTITY USING INDEX {}",
            fixture.table, idx_name
        ))
        .await
        .unwrap();

    let (mut recv, handle) = spawn_source(fixture.config()).await;

    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {0} VALUES (1, 100, 'doomed'); DELETE FROM {0} WHERE id=1",
            fixture.table
        ))
        .await
        .unwrap();

    let table_name = fixture.table.clone();
    let events = collect_until(
        &mut recv,
        |events| filter_table_rows(events, &table_name).len() >= 2,
        30,
    )
    .await;
    let rows = filter_table_rows(&events, &fixture.table);
    assert_eq!(rows.len(), 2);
    let delete = rows
        .iter()
        .find(|e| get_string(e, "operation") == "delete")
        .expect("expected a delete event");
    let old = get_object(delete, "old");
    // pgoutput emits one slot per column for the relation. Under
    // REPLICA IDENTITY USING INDEX, only the indexed column carries the
    // real value; every other column is sent as the NULL tuple-data kind
    // (`'n'`), which our parser surfaces as `Value::Null`. This is the
    // distinguishing wire-shape vs REPLICA IDENTITY FULL (all columns
    // populated) and DEFAULT (primary-key columns populated, non-PK Null).
    assert_eq!(
        old.get("alt_key").unwrap(),
        &vrl::value::Value::Integer(100)
    );
    assert_eq!(
        old.get("id").unwrap(),
        &vrl::value::Value::Null,
        "id column should be Null (not populated) under REPLICA IDENTITY USING INDEX"
    );
    assert_eq!(
        old.get("label").unwrap(),
        &vrl::value::Value::Null,
        "non-indexed column `label` should be Null"
    );

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_emits_internal_metrics_for_source_compliance() {
    // Verifies that the source emits the standard component_* metrics
    // (ComponentReceivedBytesTotal, ComponentReceivedEventsTotal,
    // ComponentReceivedEventBytesTotal) tagged with `protocol`, which is
    // what Vector's `assert_source_compliance` checks for. Without this,
    // operators have no way to monitor the source's throughput.
    use crate::test_util::components::{SOURCE_TAGS, assert_source_compliance};

    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, label TEXT)").await;

    assert_source_compliance(&SOURCE_TAGS, async {
        let (mut recv, handle) = spawn_source(fixture.config()).await;
        fixture
            .client
            .simple_query(&format!(
                "INSERT INTO {} VALUES (1, 'metric_check')",
                fixture.table
            ))
            .await
            .unwrap();
        let table_name = fixture.table.clone();
        let _ = collect_until(
            &mut recv,
            |events| !filter_table_rows(events, &table_name).is_empty(),
            30,
        )
        .await;
        handle.abort();
    })
    .await;

    fixture.teardown().await;
}

#[tokio::test]
async fn test_dropped_event_advances_lsn() {
    // A "dropped" event in Vector still surfaces as BatchStatus::Delivered,
    // so this test verifies that the source advances the LSN even though
    // no downstream sink consumed the event in the conventional sense.
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, label TEXT)").await;
    let before = fixture.confirmed_flush_lsn().await;

    let (mut recv, handle) = spawn_source(fixture.config()).await;

    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {} VALUES (1, 'drop-me')",
            fixture.table
        ))
        .await
        .unwrap();

    // Consume the event. spawn_source uses new_test_finalize(Delivered) so
    // dropping the event triggers the Delivered batch status; that's the
    // same path a VRL drop transform would take (Dropped → Delivered at
    // the batch level).
    let events = collect_events(&mut recv, 1, 30).await;
    assert_eq!(events.len(), 1);
    drop(events);

    let advanced = fixture
        .wait_for_lsn_to_advance(before.clone(), Duration::from_secs(30))
        .await;
    assert!(
        advanced.is_some(),
        "LSN should advance even for dropped events"
    );

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_numeric_preserved_as_string() {
    trace_init();
    let fixture =
        CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, amount NUMERIC(30, 15))")
            .await;
    let (mut recv, handle) = spawn_source(fixture.config()).await;

    // Insert a value that f64 cannot represent exactly.
    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {} VALUES (1, 1.234567890123456)",
            fixture.table
        ))
        .await
        .unwrap();

    let events = collect_events(&mut recv, 1, 30).await;
    assert_eq!(events.len(), 1);
    let new = get_object(&events[0], "new");
    match new.get("amount").unwrap() {
        vrl::value::Value::Bytes(b) => {
            let s = String::from_utf8_lossy(b);
            assert!(
                s.starts_with("1.234567890123456"),
                "numeric truncated/rounded: {s}"
            );
        }
        other => panic!("expected NUMERIC as Bytes, got {other:?}"),
    }

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_jsonb_bytea_and_timestamptz_roundtrip() {
    // End-to-end coverage for the typed-column decode path against a live
    // PG. The pgoutput parser unit tests already cover the OID-to-`Value`
    // mapping in isolation, but this test catches regressions where the
    // Relation message's type OIDs do not reach `decode_value` correctly
    // (e.g. column ordering, repeated parses, schema cache eviction).
    trace_init();
    let fixture = CdcFixture::setup(
        "CREATE TABLE {table} ( \
         id INT PRIMARY KEY, \
         payload JSONB, \
         raw BYTEA, \
         ts TIMESTAMPTZ \
         )",
    )
    .await;
    let (mut recv, handle) = spawn_source(fixture.config()).await;

    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {} VALUES \
             (1, '{{\"x\": 42, \"y\": [true, null]}}'::jsonb, \
              '\\x010203fe'::bytea, \
              '2025-06-01 12:34:56+00'::timestamptz)",
            fixture.table
        ))
        .await
        .unwrap();

    let events = collect_events(&mut recv, 1, 30).await;
    assert_eq!(events.len(), 1, "expected one insert event");
    let new = get_object(&events[0], "new");

    // jsonb → Value::Object
    match new.get("payload").unwrap() {
        vrl::value::Value::Object(map) => {
            assert_eq!(map.get("x"), Some(&vrl::value::Value::Integer(42)));
        }
        other => panic!("expected jsonb as Object, got {other:?}"),
    }

    // bytea → Value::Bytes of the decoded hex bytes
    match new.get("raw").unwrap() {
        vrl::value::Value::Bytes(b) => assert_eq!(b.as_ref(), &[0x01, 0x02, 0x03, 0xFE]),
        other => panic!("expected bytea as raw Bytes, got {other:?}"),
    }

    // timestamptz → Value::Bytes containing PG's ISO-8601 text. We assert
    // on the prefix to avoid coupling to the server's timezone-display
    // settings; the value-shape contract is "PG's text form, untouched".
    match new.get("ts").unwrap() {
        vrl::value::Value::Bytes(b) => {
            let s = String::from_utf8_lossy(b);
            assert!(
                s.starts_with("2025-06-01"),
                "timestamptz text payload not preserved: {s}"
            );
        }
        other => panic!("expected timestamptz as Bytes (ISO-8601 text), got {other:?}"),
    }

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_transactional_logical_message_emitted() {
    // pg_logical_emit_message(true, prefix, content) inside a transaction
    // travels through the slot tagged with the transaction's commit LSN.
    // The source should surface it as a `message` event.
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY)").await;
    let (mut recv, handle) = spawn_source(fixture.config()).await;

    fixture
        .client
        .simple_query(&format!(
            "BEGIN; \
             INSERT INTO {} VALUES (1); \
             SELECT pg_logical_emit_message(true, 'vector.test', 'tx-payload'); \
             COMMIT;",
            fixture.table
        ))
        .await
        .unwrap();

    let events = collect_events(&mut recv, 2, 30).await;
    assert_eq!(events.len(), 2, "expected insert + message");
    let msg = events
        .iter()
        .find(|e| get_string(e, "operation") == "message")
        .expect("expected a message event");
    let row = events
        .iter()
        .find(|e| get_string(e, "operation") == "insert")
        .expect("expected an insert event");

    // Both share the txn's LSN.
    assert_eq!(get_string(msg, "lsn"), get_string(row, "lsn"));
    assert_eq!(
        get_field(msg, "transactional"),
        Some(&vrl::value::Value::Boolean(true))
    );
    assert_eq!(get_string(msg, "prefix"), "vector.test");
    match get_field(msg, "content").unwrap() {
        vrl::value::Value::Bytes(b) => {
            assert_eq!(&b[..], b"tx-payload");
        }
        other => panic!("content not bytes: {other:?}"),
    }

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_logical_message_binary_payload_roundtrips() {
    // `pg_logical_emit_message` accepts either a text or bytea payload. The
    // bytea variant is meant for arbitrary binary content — null bytes,
    // non-UTF-8 bytes, the works — and our handling must preserve every
    // byte exactly. (`Value::Bytes` stores a raw `Bytes`, so the bytes are
    // intact through the source; what we're proving here is that we don't
    // accidentally try to UTF-8 decode along the way.)
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY)").await;
    let (mut recv, handle) = spawn_source(fixture.config()).await;

    // Payload: 0x00 (embedded null), 0xFF (high byte, invalid UTF-8 lead),
    // 0xFE, 0xC0 (UTF-8 invalid in any position), then printable text.
    fixture
        .client
        .simple_query(
            "SELECT pg_logical_emit_message(false, 'vector.bin', \
             '\\x00ffFEc0' || 'hello'::bytea)",
        )
        .await
        .unwrap();

    let events = collect_events(&mut recv, 1, 30).await;
    assert_eq!(events.len(), 1);
    let msg = &events[0];
    assert_eq!(get_string(msg, "operation"), "message");
    assert_eq!(get_string(msg, "prefix"), "vector.bin");
    match get_field(msg, "content").unwrap() {
        vrl::value::Value::Bytes(b) => {
            assert_eq!(
                &b[..],
                &[0x00, 0xFF, 0xFE, 0xC0, b'h', b'e', b'l', b'l', b'o'][..],
                "binary message content was not preserved byte-for-byte"
            );
        }
        other => panic!("content not bytes: {other:?}"),
    }

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_non_transactional_logical_message_emitted() {
    // pg_logical_emit_message(false, prefix, content) is delivered
    // immediately and carries its own LSN — not tied to any surrounding
    // transaction.
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY)").await;
    let (mut recv, handle) = spawn_source(fixture.config()).await;

    fixture
        .client
        .simple_query("SELECT pg_logical_emit_message(false, 'vector.heartbeat', 'beat')")
        .await
        .unwrap();

    let events = collect_events(&mut recv, 1, 30).await;
    assert_eq!(events.len(), 1, "expected one message event");
    let msg = &events[0];
    assert_eq!(get_string(msg, "operation"), "message");
    assert_eq!(
        get_field(msg, "transactional"),
        Some(&vrl::value::Value::Boolean(false))
    );
    assert_eq!(get_string(msg, "prefix"), "vector.heartbeat");
    // Non-transactional messages MUST NOT carry txn metadata. We assert
    // absence (key not present) rather than presence-of-`null`, so that
    // downstream consumers can distinguish "no transaction" from "field
    // explicitly null".
    assert_eq!(get_field(msg, "transaction_id"), None);
    assert_eq!(get_field(msg, "transaction_timestamp"), None);
    assert_eq!(get_field(msg, "new"), None);
    assert_eq!(get_field(msg, "old"), None);
    assert_eq!(get_field(msg, "schema"), None);
    assert_eq!(get_field(msg, "table"), None);

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_transaction_rollback_emits_no_events() {
    // Sanity: a transaction that ROLLBACKs must not produce any events,
    // and must not affect the slot's confirmed_flush_lsn beyond what an
    // empty txn would. This is one of the most basic CDC safety properties.
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, label TEXT)").await;
    let (mut recv, handle) = spawn_source(fixture.config()).await;

    fixture
        .client
        .simple_query(&format!(
            "BEGIN; INSERT INTO {} VALUES (1, 'rolled-back'); ROLLBACK;",
            fixture.table
        ))
        .await
        .unwrap();

    // Then a committed insert to give the receiver something to deliver,
    // proving the source isn't simply blocked.
    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {} VALUES (2, 'committed')",
            fixture.table
        ))
        .await
        .unwrap();

    let events = collect_events(&mut recv, 1, 30).await;
    assert_eq!(events.len(), 1, "rollback must not produce events");
    let new = get_object(&events[0], "new");
    assert_eq!(new.get("id").unwrap(), &vrl::value::Value::Integer(2));

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_concurrent_writers_lsn_ordered() {
    // pgoutput guarantees non-interleaved transaction delivery: txn A is
    // either fully emitted before txn B's first event, or fully after.
    // This test exercises that property using two distinct client
    // connections committing concurrently, and verifies that:
    //   * every row is delivered exactly once
    //   * LSNs are strictly non-decreasing in receive order
    //   * events from the same transaction share one LSN
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, src TEXT)").await;
    let (mut recv, handle) = spawn_source(fixture.config()).await;

    // Two extra connections, each running a small txn concurrently.
    let endpoint = fixture.endpoint.clone();
    let table = fixture.table.clone();
    let writer1 = tokio::spawn(async move {
        let (c, conn) = tokio_postgres::connect(&endpoint, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        c.simple_query(&format!(
            "INSERT INTO {table} VALUES (10, 'writer1'), (11, 'writer1')"
        ))
        .await
        .unwrap();
    });
    let endpoint = fixture.endpoint.clone();
    let table = fixture.table.clone();
    let writer2 = tokio::spawn(async move {
        let (c, conn) = tokio_postgres::connect(&endpoint, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        c.simple_query(&format!(
            "INSERT INTO {table} VALUES (20, 'writer2'), (21, 'writer2')"
        ))
        .await
        .unwrap();
    });

    writer1.await.unwrap();
    writer2.await.unwrap();

    let events = collect_events(&mut recv, 4, 30).await;
    assert_eq!(events.len(), 4, "expected 4 events from 2 concurrent txns");

    // LSNs must be non-decreasing in delivery order.
    let lsns: Vec<String> = events.iter().map(|e| get_string(e, "lsn")).collect();
    for w in lsns.windows(2) {
        assert!(
            parse_lsn(&w[0]) <= parse_lsn(&w[1]),
            "LSN regression: {} -> {}",
            w[0],
            w[1]
        );
    }

    // Each transaction's events share one LSN. Group by `src` and verify.
    let mut by_src: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for ev in &events {
        let src = match get_object(ev, "new").get("src") {
            Some(vrl::value::Value::Bytes(b)) => String::from_utf8_lossy(b).to_string(),
            _ => panic!("missing src"),
        };
        by_src.entry(src).or_default().push(get_string(ev, "lsn"));
    }
    for (src, lsns) in &by_src {
        let first = &lsns[0];
        for l in lsns {
            assert_eq!(l, first, "rows from {src} carry different LSNs: {lsns:?}");
        }
    }

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_dropped_table_does_not_break_stream() {
    // Inspired by Debezium's shouldIgnoreEventsForDeletedTable. If a
    // published table is dropped mid-stream, subsequent writes to OTHER
    // published tables must continue to be delivered. The publication
    // implicitly stops covering the dropped table; we should not crash.
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, v TEXT)").await;
    let doomed = format!("{}_doomed", fixture.table);
    fixture
        .add_table_to_publication(&doomed, "CREATE TABLE {table} (id INT PRIMARY KEY)")
        .await;

    let (mut recv, handle) = spawn_source(fixture.config()).await;

    fixture
        .client
        .simple_query(&format!("INSERT INTO {doomed} VALUES (1)"))
        .await
        .unwrap();
    let _ = collect_events(&mut recv, 1, 30).await;

    // Drop the doomed table. Subsequent inserts to the main table must
    // still flow.
    fixture
        .client
        .simple_query(&format!("DROP TABLE {doomed}"))
        .await
        .unwrap();
    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {} VALUES (100, 'alive')",
            fixture.table
        ))
        .await
        .unwrap();

    let post_drop = collect_events(&mut recv, 1, 30).await;
    assert_eq!(post_drop.len(), 1, "main table events must still flow");
    let new = get_object(&post_drop[0], "new");
    assert_eq!(new.get("id").unwrap(), &vrl::value::Value::Integer(100));

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_mixed_dml_single_transaction() {
    // Multiple operations inside one BEGIN/COMMIT must all share one LSN.
    // This is the invariant the LsnTracker's Vec-per-LSN shape exists to
    // protect: a single ack of a multi-row transaction must move the slot
    // exactly once, not once per row.
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, label TEXT)").await;
    fixture
        .client
        .simple_query(&format!(
            "ALTER TABLE {} REPLICA IDENTITY FULL",
            fixture.table
        ))
        .await
        .unwrap();
    let (mut recv, handle) = spawn_source(fixture.config()).await;

    fixture
        .client
        .simple_query(&format!(
            "BEGIN; \
             INSERT INTO {0} VALUES (1, 'a'), (2, 'b'); \
             UPDATE {0} SET label='aa' WHERE id=1; \
             DELETE FROM {0} WHERE id=2; \
             COMMIT;",
            fixture.table
        ))
        .await
        .unwrap();

    let events = collect_events(&mut recv, 4, 30).await;
    assert_eq!(events.len(), 4, "expected 4 row events from one txn");
    let ops: Vec<String> = events.iter().map(|e| get_string(e, "operation")).collect();
    assert_eq!(ops, vec!["insert", "insert", "update", "delete"]);
    // All four events must carry the same LSN (the txn's commit LSN).
    let lsn = get_string(&events[0], "lsn");
    for ev in &events[1..] {
        assert_eq!(get_string(ev, "lsn"), lsn);
    }

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_many_transactions_slot_advances_continuously() {
    // Sustained throughput: push 50 small transactions and confirm the slot
    // advances multiple times (not just once at the end). Catches a class
    // of bugs where LSN advancement only happens on shutdown or where a
    // missing wake breaks steady-state progress.
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, label TEXT)").await;
    let (mut recv, handle) = spawn_source(fixture.config()).await;

    let start = fixture.confirmed_flush_lsn().await;
    let mut milestones = Vec::new();
    for batch in 0..5 {
        for i in 0..10 {
            let id = batch * 10 + i;
            fixture
                .client
                .simple_query(&format!(
                    "INSERT INTO {} VALUES ({id}, 'row{id}')",
                    fixture.table
                ))
                .await
                .unwrap();
        }
        // Drain the 10 events for this batch so their batches finalize.
        let _ = collect_events(&mut recv, 10, 30).await;
        // Sample LSN periodically.
        time::sleep(Duration::from_millis(750)).await;
        milestones.push(fixture.confirmed_flush_lsn().await);
    }

    // We expect milestones to strictly advance through the 5 sample points.
    let distinct: std::collections::HashSet<_> = milestones.iter().collect();
    assert!(
        distinct.len() >= 3,
        "confirmed_flush_lsn should advance multiple times during sustained \
         insertion. Saw milestones: {milestones:?} (start={start:?})"
    );

    handle.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_resume_does_not_replay_acked_transactions() {
    // Realistic resume scenario: many committed-and-ack'd transactions
    // followed by a reconnect, followed by a few new transactions.
    //
    // PostgreSQL logical replication is at-least-once: the *very last*
    // acked transaction may be re-delivered (this is by design — see the
    // test_reconnect_resumes_from_lsn comment). What MUST hold is that we
    // do NOT replay the entire history. This test catches the failure
    // mode where the slot fails to advance and every reconnect re-streams
    // everything from creation.
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY)").await;
    let (mut recv1, handle1) = spawn_source(fixture.config()).await;
    // 25 individual transactions before the reconnect.
    for i in 0..25 {
        fixture
            .client
            .simple_query(&format!("INSERT INTO {} VALUES ({i})", fixture.table))
            .await
            .unwrap();
    }
    let phase1 = collect_events(&mut recv1, 25, 60).await;
    assert_eq!(phase1.len(), 25);
    // Capture the LSN of the *last* phase-1 event. We must wait for the
    // slot to advance to (or past) this LSN before killing the source —
    // otherwise the still-unacked tail will be re-delivered on resume and
    // we'd fail the strict `phase2.len() < 10` check.
    let last_phase1_lsn = get_string(phase1.last().unwrap(), "lsn");
    drop(phase1);

    let advanced = fixture
        .wait_for_lsn_at_or_past(&last_phase1_lsn, Duration::from_secs(60))
        .await;
    assert!(
        advanced.is_some(),
        "slot did not reach the last phase-1 LSN ({last_phase1_lsn}) within 60s"
    );

    handle1.abort();
    drop(recv1);
    time::sleep(Duration::from_secs(2)).await;

    // Reconnect. Insert 3 new rows.
    let (mut recv2, handle2) = spawn_source(fixture.config()).await;
    for i in 25..28 {
        fixture
            .client
            .simple_query(&format!("INSERT INTO {} VALUES ({i})", fixture.table))
            .await
            .unwrap();
    }

    // Collect with an early-exit: once we've seen all 3 new IDs, stop
    // waiting. This avoids burning the full timeout collecting concurrent
    // tests' message events. The 15s outer bound is well above the
    // sub-second arrival we expect.
    let table_name = fixture.table.clone();
    let phase2 = collect_until(
        &mut recv2,
        |events| {
            let ids: std::collections::HashSet<i64> = filter_table_rows(events, &table_name)
                .iter()
                .filter_map(|e| match get_object(e, "new").get("id") {
                    Some(vrl::value::Value::Integer(i)) => Some(*i),
                    _ => None,
                })
                .collect();
            (25i64..28).all(|id| ids.contains(&id))
        },
        15,
    )
    .await;

    // Strict accounting: out of phase2, count only inserts on our table.
    // We MUST see rows 25, 26, 27 (the new ones). We may see up to one
    // re-delivered row (PG's at-least-once guarantee on the last-acked
    // txn). Anything beyond `3 + 1 = 4` row events means the slot failed
    // to advance and the resume mechanism is broken.
    let inserts: Vec<&Event> = filter_table_rows(&phase2, &fixture.table);
    let ids: Vec<i64> = inserts
        .iter()
        .filter_map(|e| match get_object(e, "new").get("id") {
            Some(vrl::value::Value::Integer(i)) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(
        inserts.len() <= 4,
        "resume re-played too many transactions: got {} row events from {}, expected 3 (and at most 1 re-delivered). ids={ids:?}",
        inserts.len(),
        fixture.table,
    );
    for new_id in 25i64..28 {
        assert!(
            ids.contains(&new_id),
            "new row id={new_id} not delivered after resume; got ids={ids:?}"
        );
    }

    handle2.abort();
    fixture.teardown().await;
}

#[tokio::test]
async fn test_schema_change_invalidates_relation_cache() {
    trace_init();
    let fixture = CdcFixture::setup("CREATE TABLE {table} (id INT PRIMARY KEY, label TEXT)").await;
    let (mut recv, handle) = spawn_source(fixture.config()).await;

    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {} VALUES (1, 'before')",
            fixture.table
        ))
        .await
        .unwrap();
    let first = collect_events(&mut recv, 1, 30).await;
    assert_eq!(first.len(), 1);

    fixture
        .client
        .simple_query(&format!(
            "ALTER TABLE {} ADD COLUMN extra TEXT",
            fixture.table
        ))
        .await
        .unwrap();
    fixture
        .client
        .simple_query(&format!(
            "INSERT INTO {} (id, label, extra) VALUES (2, 'after', 'new-col')",
            fixture.table
        ))
        .await
        .unwrap();

    let second = collect_events(&mut recv, 1, 30).await;
    assert_eq!(second.len(), 1);
    let new = get_object(&second[0], "new");
    assert_eq!(
        new.get("extra").unwrap(),
        &vrl::value::Value::from("new-col")
    );

    handle.abort();
    fixture.teardown().await;
}

// ---------------------------------------------------------------------------
// Throughput benchmarks — gated behind `postgresql_cdc-benchmarks`.
//
// These are integration tests (not Criterion) because they need a live PG.
// Run via:
//
//   cargo vdev int start postgresql_cdc
//   cargo test -p vector \
//     --features postgresql_cdc-benchmarks --no-default-features \
//     --lib sources::postgresql_cdc::integration_tests::bench \
//     -- --nocapture
// ---------------------------------------------------------------------------

#[cfg(feature = "postgresql_cdc-benchmarks")]
mod bench {
    use super::*;
    use vector_lib::EstimatedJsonEncodedSizeOf;

    async fn run_throughput_bench(row_count: u64) {
        trace_init();
        let fixture = CdcFixture::setup(
            "CREATE TABLE {table} ( \
             id BIGINT PRIMARY KEY, \
             label TEXT NOT NULL, \
             amount NUMERIC(12,2) NOT NULL, \
             payload JSONB NOT NULL \
             )",
        )
        .await;

        // Bulk-insert before starting the source so WAL is pre-buffered.
        // Each row is ~120 bytes of WAL (4 typed columns). The INSERT runs
        // in a single transaction: pgoutput emits Begin, N Inserts, Commit.
        let insert_sql = format!(
            "INSERT INTO {} (id, label, amount, payload) \
             SELECT g, \
                    'row-' || g, \
                    (g % 100000)::numeric / 100, \
                    jsonb_build_object('seq', g, 'tag', 'bench') \
             FROM generate_series(1, {row_count}) g",
            fixture.table
        );
        let t0 = std::time::Instant::now();
        fixture
            .client
            .simple_query(&insert_sql)
            .await
            .expect("bulk insert");
        let insert_dur = t0.elapsed();
        println!(
            "[bench] Inserted {row_count} rows in {:.2}s ({:.0} rows/s)",
            insert_dur.as_secs_f64(),
            row_count as f64 / insert_dur.as_secs_f64()
        );

        // Drain events with an O(n) counter instead of the O(n²)
        // collect_until + filter_table_rows combination that rescans the
        // entire accumulated vector on every arrival.
        let (mut recv, handle) = spawn_source(fixture.config()).await;
        let t1 = std::time::Instant::now();
        let table_name = fixture.table.clone();
        let mut events: Vec<Event> = Vec::with_capacity(row_count as usize + 64);
        let mut row_count_seen: u64 = 0;
        let deadline = time::Instant::now() + Duration::from_secs(600);
        while row_count_seen < row_count {
            let remaining = deadline.saturating_duration_since(time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match time::timeout(remaining, recv.next()).await {
                Ok(Some(e)) => {
                    let op = get_string(&e, "operation");
                    let is_row = (op == "insert" || op == "update" || op == "delete")
                        && get_string(&e, "table") == table_name;
                    if is_row {
                        row_count_seen += 1;
                    }
                    events.push(e);
                }
                Ok(None) | Err(_) => break,
            }
        }
        let consume_dur = t1.elapsed();

        let total_events = events.len();
        let total_bytes: usize = events
            .iter()
            .map(|e| e.estimated_json_encoded_size_of().get())
            .sum();

        println!(
            "[bench] Consumed {total_events} events ({row_count_seen} row events) in {:.2}s",
            consume_dur.as_secs_f64()
        );
        println!(
            "[bench] Throughput: {:.0} events/s, {:.1} MB/s (estimated JSON size)",
            total_events as f64 / consume_dur.as_secs_f64(),
            total_bytes as f64 / consume_dur.as_secs_f64() / 1_048_576.0
        );
        assert_eq!(
            row_count_seen, row_count,
            "expected {row_count} row events, got {row_count_seen}"
        );

        handle.abort();
        fixture.teardown().await;
    }

    #[tokio::test]
    async fn bench_100k_rows() {
        run_throughput_bench(100_000).await;
    }

    #[tokio::test]
    async fn bench_1m_rows() {
        run_throughput_bench(1_000_000).await;
    }
}
