//! PostgreSQL logical replication source using the `pgoutput` plugin.
//!
//! Streams INSERT / UPDATE / DELETE / TRUNCATE events from a PostgreSQL
//! database via logical replication. The source requires an existing
//! publication and replication slot using the `pgoutput` output plugin —
//! see the configuration documentation for setup instructions.

mod config;
mod lsn_tracker;
mod pgoutput;
mod source;

#[cfg(all(test, feature = "postgresql_cdc-integration-tests"))]
mod integration_tests;

pub use config::PostgresqlCdcConfig;
