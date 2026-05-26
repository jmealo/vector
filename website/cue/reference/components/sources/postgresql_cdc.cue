package metadata

components: sources: postgresql_cdc: {
	title: "PostgreSQL CDC"

	description: """
		Streams change-data-capture events from a [PostgreSQL](\(urls.postgresql)) database using the
		built-in logical replication protocol (`pgoutput`). Captures INSERTs, UPDATEs, DELETEs, and
		TRUNCATEs along with the transaction context they belong to, and integrates with Vector's
		end-to-end acknowledgement system so the slot's `confirmed_flush_lsn` only advances after the
		downstream sink has confirmed durable delivery.
		"""

	classes: {
		commonly_used: false
		delivery:      "at_least_once"
		deployment_roles: ["aggregator", "sidecar"]
		development:   "beta"
		egress_method: "stream"
		stateful:      true
	}

	features: {
		auto_generated:   true
		acknowledgements: true
		collect: {
			checkpoint: enabled: true
			tls: {
				enabled:                true
				can_verify_certificate: true
				can_verify_hostname:    true
				enabled_default:        false
				enabled_by_scheme:      false
			}
			from: {
				service: services.postgres
				interface: {
					socket: {
						direction: "outgoing"
						protocols: ["tcp"]
						ssl: "optional"
					}
				}
			}
		}
		multiline: enabled: false
	}

	support: {
		requirements: [
			"""
				PostgreSQL 10 or newer is required — the minimum version that supports logical
				replication via the `pgoutput` plugin. The source uses `pg_replication_slots.wal_status`
				for slot-health diagnostics when available; that column was added in PostgreSQL 13,
				and on older servers the diagnostic is degraded to `"unknown"` while the rest of the
				source continues to function. The `SlotWalLost` startup error is therefore
				unavailable on PostgreSQL 12 and earlier; monitor slot lag out-of-band on those
				versions (for example via the `postgresql_metrics` source).
				""",
			"""
				The PostgreSQL server must have `wal_level = logical` set, plus `max_replication_slots`
				and `max_wal_senders` configured high enough to accommodate this source. Restart the
				server after changing `wal_level`.
				""",
			"""
				A logical replication slot using the `pgoutput` output plugin must already exist before
				the source starts. Create it once via `SELECT pg_create_logical_replication_slot('<slot>',
				'pgoutput');`. This source intentionally does not create or drop slots — that is an
				operator responsibility, because an abandoned slot can cause unbounded WAL growth and
				eventually take the database down. On startup the source validates the slot:
				it must exist, use the `pgoutput` plugin, not be attached to another consumer, and
				not be in a `lost` WAL state. Each of those conditions surfaces as a clear
				configuration-load error rather than a silent failure later.
				""",
			"""
				A publication that includes every table you want to replicate must exist:
				`CREATE PUBLICATION <pub> FOR TABLE table1, table2, ...;`.
				""",
			"""
				The configured user must have the `REPLICATION` role attribute (or be a superuser).
				Use the minimum-privilege role described below — `SUPERUSER` is **not** required and
				should be avoided in production.

				```sql
				-- 1. Create a role with replication privileges and a password.
				CREATE ROLE vector_cdc WITH LOGIN REPLICATION PASSWORD 'change_me';

				-- 2. Allow the role to connect to the database.
				GRANT CONNECT ON DATABASE app TO vector_cdc;

				-- 3. Grant SELECT on the tables the publication covers. SELECT is what
				--    pgoutput uses to read the row payloads from the table heap when
				--    a transaction commits; without it, the slot will fail to decode.
				GRANT USAGE ON SCHEMA public TO vector_cdc;
				GRANT SELECT ON TABLE public.orders, public.customers TO vector_cdc;

				-- 4. If you use row-level security, the publication must be created
				--    with `(publish_via_partition_root = true)` and the role needs
				--    `BYPASSRLS`, or you must add explicit RLS policies that allow
				--    `vector_cdc` to read the rows you intend to replicate.
				```

				The role does **not** need `CREATEDB`, `CREATEROLE`, or any table-level
				INSERT/UPDATE/DELETE privileges.
				""",
		]

		warnings: [
			"""
				`UPDATE` and `DELETE` events only include the row's primary-key columns by default. To
				receive the full pre-image of the row in the `old` field, set the table's replica identity
				to `FULL`: `ALTER TABLE <table> REPLICA IDENTITY FULL;`. This has a moderate write-amplification cost.
				""",
			"""
				PostgreSQL's logical replication is at-least-once. After a Vector restart the last
				transaction that was acknowledged before the disconnect may be re-delivered. Downstream
				sinks or transforms that need exactly-once semantics should dedupe on the `lsn` field.
				""",
		]
		notices: []
	}

	installation: {
		platform_name: null
	}

	configuration: generated.components.sources.postgresql_cdc.configuration

	how_it_works: {
		lsn_acknowledgement: {
			title: "End-to-end LSN acknowledgement"
			body: """
				PostgreSQL's logical replication is driven by *Log Sequence Numbers* (LSNs). The slot has
				two LSN positions:

				- `restart_lsn` — the earliest WAL segment Postgres must retain for this slot.
				- `confirmed_flush_lsn` — the position the consumer has confirmed as durable.

				Vector advances `confirmed_flush_lsn` only after every event from a given transaction has
				been acknowledged by the downstream sink. This is enforced by attaching a per-batch
				notifier to every event emitted from a transaction and waiting for *all* of them to
				resolve before reporting that transaction's commit LSN as the new applied position.

				A long-running transaction with 10,000 row changes therefore produces 10,000 events all
				tagged with the same `lsn`, and the slot only advances once for the whole transaction.
				A `BatchNotifier` whose sender drops without ever firing (which can happen if a
				downstream component is reconfigured mid-stream) is treated the same as `Delivered`
				for LSN-advancement purposes, matching the behaviour of the `kafka` source.

				If the sink reports `Errored` or `Rejected` for any batch, the source halts LSN
				advancement, surfaces the error in the logs, and stops accepting new events from the
				replication stream. The slot remains pinned at the last successfully acknowledged LSN,
				preventing data loss.
				"""
		}

		transaction_boundaries: {
			title: "Transaction boundaries"
			body: """
				`pgoutput` emits transactions atomically and in commit order. You will always see the
				complete sequence `Begin → row events → Commit` for one transaction before any messages
				from the next. The source uses this guarantee to attach the transaction's metadata
				(`transaction_id`, `transaction_timestamp`, `lsn`) to every row event without needing to
				match begin/commit explicitly.
				"""
		}

		data_types: {
			title: "Column type mapping"
			body: """
				`pgoutput` emits column values in PostgreSQL's text representation by default. The source
				maps them to Vector's native `Value` variants as follows:

				| PostgreSQL type                     | Vector field type     | Notes                                                                                  |
				|-------------------------------------|-----------------------|----------------------------------------------------------------------------------------|
				| `bool`                              | boolean               | `t` / `f` from the wire                                                                |
				| `int2`, `int4`, `int8`              | integer               | Parsed as `i64`. Values outside the i64 range fall back to the raw text string.        |
				| `float4`, `float8`                  | float                 | Parsed as `f64`. `NaN`/`Inf` fall back to the raw text string.                         |
				| `numeric`                           | string                | **Preserved exactly as text.** Casting to float would silently lose precision.         |
				| `text`, `varchar`, `char`, `name`   | string                | Raw UTF-8 bytes; stored internally as `Value::Bytes` and serialized as a JSON string by downstream sinks. |
				| `bytea`                             | bytes                 | Decoded from the wire's `\\x…` hex form into raw bytes.                                |
				| `json`, `jsonb`                     | object                | Parsed with `serde_json`. Malformed JSON falls back to the raw text string.            |
				| `date`, `timestamp`, `timestamptz`  | string                | ISO-8601 text from PostgreSQL, stored as `Value::Bytes`. Use `parse_timestamp` in VRL to convert if needed. |
				| `uuid`, `inet`, `cidr`, `macaddr`, `interval` and other typed columns | string  | Surface as their text representation (`Value::Bytes`); convert in VRL if you need typed values. |

				Two type-handling rules drive this table:

				- `numeric` columns are kept as strings so arbitrary-precision values are never silently
				  rounded. If you need float arithmetic, do it explicitly in a VRL transform:
				  `.new.amount = to_float!(.new.amount)`.
				- `bytea` columns are passed through as raw bytes, not base64-encoded. The encoding
				  decision belongs to the downstream sink (or a VRL `encode_base64` call), not this source.

				### TOAST'd columns

				When PostgreSQL elects not to re-send a TOAST'd value because it did not change in an
				UPDATE, the source omits that column from the `new` (and `old`) payload entirely. The
				omitted column names are reported in a metadata field `__toast_omitted` so downstream
				consumers can distinguish "value was unchanged" from "value is missing":

				```json
				{
				  "operation": "update",
				  "new": { "id": 42, "status": "complete" },
				  "old": { "id": 42 },
				  "__toast_omitted": ["large_text_body"]
				}
				```

				This avoids the common antipattern of injecting a sentinel string like
				`"__toast_unchanged__"` into a column whose downstream schema may expect an integer or
				bytea — which would cause coercion failures in strict sinks such as ClickHouse.
				"""
		}

		generic_wal_messages: {
			title: "Generic WAL messages (`pg_logical_emit_message`)"
			body: """
				PostgreSQL exposes a `pg_logical_emit_message(transactional bool, prefix text, content
				text-or-bytea)` function for embedding application-level markers into the WAL stream. The
				source surfaces these as `operation = "message"` events:

				| Field                   | Description                                                                |
				|-------------------------|----------------------------------------------------------------------------|
				| `operation`             | Literal `"message"`                                                        |
				| `lsn`                   | The message's WAL position (or the transaction's commit LSN if it is transactional) |
				| `transactional`         | `true` if emitted inside a transaction, `false` otherwise                  |
				| `prefix`                | The application-defined prefix                                              |
				| `content`               | Raw bytes — binary-safe, including embedded NULs and non-UTF-8 sequences   |
				| `transaction_id`        | The XID; absent for non-transactional messages                              |
				| `transaction_timestamp` | Commit timestamp; absent for non-transactional messages                     |
				| `new`, `old`, `schema`, `table` | Absent on `message` events — pattern-match on `operation == "message"` rather than checking for `null` |

				Transactional messages share the enclosing transaction's commit LSN and acknowledge as
				part of that transaction. Non-transactional messages carry their own LSN and acknowledge
				independently — useful as lightweight heartbeats or stream checkpoints.
				"""
		}

		reconnection: {
			title: "Reconnection and resume semantics"
			body: """
				If the replication connection drops, Vector's topology will restart the source. On
				restart the source attaches to the same slot and passes `0/0` to `START_REPLICATION`,
				which causes PostgreSQL to resume from the slot's current `confirmed_flush_lsn`. No data
				is lost. Because PostgreSQL provides at-least-once delivery, the very last acknowledged
				transaction may be re-delivered after a restart; consumers should dedupe by `lsn` if
				exactly-once semantics are required.

				The relation cache is rebuilt from scratch on every reconnect — PostgreSQL re-sends
				`Relation` messages before resuming row events, so this happens transparently.
				"""
		}

		operational_tuning: {
			title: "Operational tuning and PostgreSQL configuration"
			body:  """
				This source's behaviour is tightly coupled to how the PostgreSQL server is
				configured. The most important relationships are summarised below; getting
				any of them wrong typically surfaces as either (a) the WAL directory growing
				until the database server runs out of disk, or (b) the source disconnecting
				under load.

				### 1. WAL retention and back-pressure (most critical)

				With `acknowledgements: true` (the default), Vector advances the slot's
				`confirmed_flush_lsn` only after every downstream sink has acknowledged the
				events in a given transaction. PostgreSQL cannot recycle WAL segments newer
				than the oldest active slot's `confirmed_flush_lsn`. The chain therefore is:

				**slow sink → Vector stops acknowledging → slot stops advancing → PG retains WAL → `pg_wal/` grows until the disk fills**.

				This is by design — it is what makes the source safe — but it means operators
				must monitor it actively. Suggested guard-rails:

				- Size sinks and Vector's buffers for your **peak** transaction volume, not the
				  average. A bulk import that doubles your normal write rate for an hour is
				  often where this fails.
				- Set `max_replication_slots` and `max_wal_senders` to at least `1` per
				  Vector instance attached to this database (more if you have other consumers).
				- Monitor the slot lag: `pg_wal_lsn_diff(pg_current_wal_lsn(),
				  confirmed_flush_lsn)` from `pg_replication_slots`. Alert when it exceeds a
				  comfortable fraction of `max_wal_size`.
				- Monitor `pg_replication_slots.active`. A slot that goes `inactive`
				  unexpectedly means Vector disconnected and the WAL is now accumulating with
				  no consumer.
				- **Observability**: Use Vector's [`postgresql_metrics`](\(urls.vector_components)/sources/postgresql_metrics)
				  source to automatically collect these metrics as `pg_replication_slots_confirmed_lag_bytes`
				  and `pg_replication_slots_active`.

				### 2. Transaction memory and spilling

				PostgreSQL 13+ exposes `logical_decoding_work_mem` (default 64MB). When a
				single transaction's decoded changes exceed this limit, PG spills them to
				disk on the server side before streaming. Vector is largely unaffected — it
				just receives the stream slightly later — but the server pays in I/O.

				If you regularly run multi-GB transactions (bulk loads, schema migrations),
				raise `logical_decoding_work_mem` to keep the spill on the server side
				bounded. Note that this source uses pgoutput protocol version 1, which
				holds the entire transaction in memory until COMMIT regardless; see the
				[protocol version section](#pgoutput-protocol-version-and-large-transaction-handling).

				### 3. Connection timeouts and heartbeats

				PostgreSQL's `wal_sender_timeout` (default 60s) terminates the replication
				connection if no client activity is observed for that long. pgwire-replication
				handles standby-status keepalives on Vector's behalf, and Vector polls its
				LSN tracker every 250ms, so this should never trigger in healthy operation.

				If you see `terminating walsender process due to replication timeout` in the
				PostgreSQL log:

				1. Check Vector's CPU. A saturated source task can delay keepalive replies.
				2. Check network latency between Vector and PG.
				3. As a last resort, raise `wal_sender_timeout` on the database, but treat
				   that as a workaround — the real fix is to address the underlying delay.

				### 4. LSN advancement cadence

				LSN advancement is **not** per-event — it is per-poll. Internally Vector polls
				its LSN tracker every 250ms and, when the tracker reports new progress,
				forwards an applied-LSN update to pgwire-replication; pgwire's own status
				interval (500ms in this source) then sends a `StandbyStatusUpdate` to the
				server. End-to-end, the slot's `confirmed_flush_lsn` updates within ~750ms
				of the last sink acknowledgement, well under PostgreSQL's
				`checkpoint_timeout` (default 5 minutes). There is virtually never a reason
				to tune the internal interval; if you find yourself wanting to, file an issue
				with the workload that motivated it.

				### 5. Recommended PostgreSQL configuration for production

				These settings together keep the WAL from running away while preserving
				replay safety:

				```
				wal_level                  = logical        # required
				max_replication_slots      = >= 1            # one per CDC consumer
				max_wal_senders            = >= 1            # one per CDC consumer
				max_wal_size               = sized for your retention window  # bounds disk pressure
				wal_sender_timeout         = 60s             # default is fine in most setups
				logical_decoding_work_mem  = 64MB or higher   # raise if you run big transactions
				```

				`wal_keep_size` (or `wal_keep_segments` on PG <13) provides a *floor* of
				WAL retention — useful for non-slot consumers, but irrelevant here: an
				active replication slot will retain WAL **indefinitely**, overriding this
				setting, until the consumer either advances `confirmed_flush_lsn` or the
				slot is dropped. Do not rely on `wal_keep_size` as a safety valve.

				### 6. Tuning summary

				| Goal               | PostgreSQL parameter      | Vector / pipeline lever            | Notes                                                                                                |
				|--------------------|---------------------------|------------------------------------|------------------------------------------------------------------------------------------------------|
				| Bound disk usage   | `max_wal_size`            | Sink throughput; `acknowledgements`| If sinks fall behind under acks, WAL grows. Size both for peak load.                                  |
				| Source can start   | `wal_level = logical`     | `replication_slot`, `publication_name` | Both must be created out-of-band before the source starts.                                       |
				| Big transactions   | `logical_decoding_work_mem` | (none — affects PG-side I/O)    | Raise on PG side to reduce spill-to-disk; Vector behaviour unchanged.                                |
				| Connection stays up| `wal_sender_timeout`      | Vector CPU allocation              | Saturated source CPU can delay heartbeats; allocate enough first, then raise the timeout if needed.  |
				| Resume after restart | `confirmed_flush_lsn`   | E2E ack pipeline                   | Source resumes from slot position; PG's at-least-once may re-deliver the last txn (dedupe by `lsn`). |
				"""
		}

		protocol_version_and_streaming: {
			title: "pgoutput protocol version and large-transaction handling"
			body: """
				This source uses pgoutput **protocol version 1**, which delivers each transaction
				atomically *after* `COMMIT`. PostgreSQL 14 introduced protocol version 2, which adds
				`StreamStart`/`StreamStop`/`StreamCommit`/`StreamAbort` messages so very large
				transactions can be decoded incrementally on the server instead of being buffered in
				shared memory until commit. The Rust client library this source builds on does not
				currently implement v2, and adopting it is non-trivial because v2 mid-transaction
				streaming interacts with Vector's end-to-end acknowledgement model:

				- A row event delivered before its enclosing transaction commits can later be aborted
				  on the server side. Once that event has been acknowledged downstream, Vector cannot
				  un-write it from the sink.

				- The two clean ways to reconcile this are (a) buffer the streamed rows on the Vector
				  side and only release them on `StreamCommit`, which gives back the latency benefit
				  but keeps ack semantics correct, or (b) propagate abort markers downstream and
				  require every sink to be 2-phase-commit aware. Vector's current sink layer does not
				  expose a 2PC contract, so (a) is the only safe path today.

				### Practical impact

				With protocol version 1 the entire transaction is held in PostgreSQL's
				`logical_decoding_work_mem`. For very large transactions (think bulk imports producing
				tens of millions of changes) this can cause server-side memory pressure or, in pre-PG
				14 versions, slot disconnects. If you regularly run such transactions, consider:

				- Chunking the work into smaller transactions.
				- Increasing `logical_decoding_work_mem` on the server (PG 13+).
				- Tracking the future `streaming_mode: "buffered_v2"` configuration option (planned),
				  which will negotiate protocol version 2 and buffer the streamed rows on the Vector
				  side, releasing them atomically on commit.
				"""
		}
	}

	output: {
		logs: change_event: {
			description: """
				A single PostgreSQL change event. One LogEvent is emitted for each row affected by an
				INSERT, UPDATE, DELETE, or TRUNCATE statement on a published table, plus one event for
				each `pg_logical_emit_message` call.
				"""
			fields: {
				operation: {
					description: "The kind of change this event describes."
					required:    true
					type: string: {
						enum: {
							insert:   "A new row was inserted."
							update:   "An existing row was modified."
							delete:   "A row was deleted."
							truncate: "The table was truncated."
							message:  "A generic WAL message emitted by `pg_logical_emit_message`."
						}
					}
				}
				schema: {
					description: "The PostgreSQL schema (namespace) of the affected table. Absent on `message` events."
					required:    false
					common:      true
					type: string: {
						default: null
						examples: ["public", "billing"]
					}
				}
				table: {
					description: "The unqualified table name of the affected relation. Absent on `message` events."
					required:    false
					common:      true
					type: string: {
						default: null
						examples: ["orders", "customers"]
					}
				}
				lsn: {
					description: """
						The PostgreSQL Log Sequence Number associated with this event, in `X/Y` hex format.
						For row events, this is the commit LSN of the enclosing transaction — every event in
						one transaction shares one `lsn`. For non-transactional WAL messages, this is the
						message's own LSN. LSNs are monotonically non-decreasing in delivery order.
						"""
					required: true
					type: string: {
						examples: ["16/B374D848", "0/1A2B3C4"]
					}
				}
				transaction_id: {
					description: "The PostgreSQL transaction id (xid) of the enclosing transaction. Absent on non-transactional `message` events."
					required:    false
					common:      true
					type: uint: {
						default: null
						unit:    null
						examples: [12345]
					}
				}
				transaction_timestamp: fields._current_timestamp & {
					description: "The commit timestamp of the enclosing transaction, in RFC 3339 format. Absent on non-transactional `message` events."
				}
				new: {
					description: """
						The new row image as an object keyed by column name. Populated for `insert` and
						`update`. `null` for `delete` and `truncate`; absent on `message` events. Columns
						omitted due to TOAST optimization are not present in this object — see the
						`__toast_omitted` field.
						"""
					required: false
					common:   true
					type: object: examples: [{id: 42, status: "pending"}]
				}
				old: {
					description: """
						The old row image. Populated for `delete` (key columns only by default, full row if
						`REPLICA IDENTITY FULL` is set) and for `update` when the table's replica identity
						captures the previous row. `null` for `insert` and `truncate`; absent on `message`
						events.
						"""
					required: false
					common:   true
					type: object: examples: [{id: 42, status: "pending"}]
				}
				operation_when_message: {
					description: """
						The following four fields are only present when `operation == "message"`.
						"""
					required: false
					common:   false
					type: object: {}
				}
				transactional: {
					description: """
						Whether this `message` event was emitted inside a transaction. Only present when
						`operation == "message"`.
						"""
					required: false
					common:   false
					type: bool: default: null
				}
				prefix: {
					description: "The application-defined prefix supplied to `pg_logical_emit_message`. Only present when `operation == \"message\"`."
					required:    false
					common:      false
					type: string: {
						default: null
						examples: ["vector.heartbeat", "myapp.checkpoint"]
					}
				}
				content: {
					description: """
						The raw bytes supplied to `pg_logical_emit_message`. Binary-safe — preserved
						byte-for-byte including embedded null bytes and non-UTF-8 sequences. Only present
						when `operation == "message"`.
						"""
					required: false
					common:   false
					type: string: {
						default: null
						examples: ["beat", "checkpoint=42"]
					}
				}
				toast_omitted: {
					description: """
						The field name is `__toast_omitted` on the LogEvent. Contains the names of columns
						that PostgreSQL did not re-send because their TOAST'd values did not change in this
						UPDATE. Only present when at least one column is omitted.
						"""
					required: false
					common:   false
					type: array: {
						default: null
						items: type: string: examples: ["large_text_body"]
					}
				}
			}
		}
	}

	telemetry: metrics: {
		component_received_bytes_total:       components.sources.internal_metrics.output.metrics.component_received_bytes_total
		component_received_events_total:      components.sources.internal_metrics.output.metrics.component_received_events_total
		component_received_event_bytes_total: components.sources.internal_metrics.output.metrics.component_received_event_bytes_total
		component_sent_events_total:          components.sources.internal_metrics.output.metrics.component_sent_events_total
		component_sent_event_bytes_total:     components.sources.internal_metrics.output.metrics.component_sent_event_bytes_total
		component_errors_total:               components.sources.internal_metrics.output.metrics.component_errors_total
		component_discarded_events_total:     components.sources.internal_metrics.output.metrics.component_discarded_events_total
	}
}
