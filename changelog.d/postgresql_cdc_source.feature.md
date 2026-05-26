Add a new `postgresql_cdc` source that streams change-data-capture events from PostgreSQL using logical replication and the built-in `pgoutput` plugin. The source attaches to an existing publication and replication slot, emits INSERT / UPDATE / DELETE / TRUNCATE events with full transaction context, and integrates with Vector's end-to-end acknowledgement system so the slot's `confirmed_flush_lsn` only advances after every event from a committed transaction has been acknowledged downstream.

authors: jmealo
