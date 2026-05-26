//! Configuration for the `postgresql_cdc` source.

use std::path::PathBuf;

use vector_lib::{
    config::{LegacyKey, LogNamespace},
    configurable::configurable_component,
    lookup::owned_value_path,
    sensitive_string::SensitiveString,
};
use vrl::value::Kind;

use crate::{
    config::{SourceAcknowledgementsConfig, SourceConfig, SourceContext, SourceOutput},
    schema,
};

/// TLS configuration for the `postgresql_cdc` source.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct PostgresqlCdcTlsConfig {
    /// Absolute path to a PEM-encoded CA certificate file used to verify the
    /// Postgres server certificate.
    #[configurable(metadata(docs::examples = "/etc/ssl/certs/ca.pem"))]
    pub ca_file: PathBuf,

    /// Whether to verify the server hostname against the certificate.
    ///
    /// Set to `false` to verify the certificate chain only (equivalent to
    /// `sslmode=verify-ca`). Default is `true` (`sslmode=verify-full`).
    #[serde(default = "default_verify_hostname")]
    pub verify_hostname: bool,
}

const fn default_verify_hostname() -> bool {
    true
}

/// Configuration for the `postgresql_cdc` source.
///
/// Streams logical replication events (INSERT / UPDATE / DELETE / TRUNCATE)
/// from a PostgreSQL database using the built-in `pgoutput` plugin. Requires
/// an existing logical replication slot and publication that the source can
/// attach to — the source will not create or drop them.
#[configurable_component(source(
    "postgresql_cdc",
    "Stream change-data-capture events from PostgreSQL using logical replication (pgoutput)."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct PostgresqlCdcConfig {
    /// libpq-style connection URI for the PostgreSQL server.
    ///
    /// Must NOT include replication-specific parameters such as `replication`
    /// or `database=replication`; the source sets those internally on the
    /// replication connection.
    #[configurable(metadata(
        docs::examples = "postgres://vector:password@db.example.com:5432/app"
    ))]
    pub connection_string: SensitiveString,

    /// Name of an existing logical replication slot that uses the `pgoutput`
    /// output plugin. The slot must be created out-of-band (e.g. via
    /// `SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput')`) —
    /// the source will not create or drop it.
    #[configurable(metadata(docs::examples = "vector_slot"))]
    pub replication_slot: String,

    /// Name of an existing publication to subscribe to. The publication
    /// determines which tables and DML operations are replicated.
    #[configurable(metadata(docs::examples = "vector_pub"))]
    pub publication_name: String,

    /// Optional LSN to start streaming from, in `X/Y` hex format.
    ///
    /// If unset, the source passes `0/0` to `START_REPLICATION`, which
    /// causes PostgreSQL to resume from the slot's `confirmed_flush_lsn`
    /// automatically. Override only for advanced replay scenarios.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "16/B374D848"))]
    pub slot_start_lsn: Option<String>,

    /// TLS configuration for the connection. If unset, the source connects
    /// without TLS.
    #[serde(default)]
    #[configurable(derived)]
    pub tls: Option<PostgresqlCdcTlsConfig>,

    /// End-to-end acknowledgement configuration.
    ///
    /// When acknowledgements are enabled (the default for this source) the
    /// confirmed-flush LSN is only advanced after every downstream sink has
    /// acknowledged the events produced by the corresponding transaction.
    #[serde(default)]
    #[configurable(derived)]
    pub acknowledgements: SourceAcknowledgementsConfig,

    /// The log namespace to use for events emitted by this source.
    ///
    /// When `true`, fields like `operation`, `lsn`, `schema`, `table`,
    /// `new`, `old`, and the transaction metadata are placed under
    /// `%postgresql_cdc.<field>` event metadata; when `false`, they are
    /// placed at the root of the `LogEvent` (the legacy shape). When
    /// unset, the source inherits the global `log_namespace` setting.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    pub log_namespace: Option<bool>,
}

impl Default for PostgresqlCdcConfig {
    fn default() -> Self {
        Self {
            connection_string: SensitiveString::default(),
            replication_slot: "vector_slot".to_owned(),
            publication_name: "vector_pub".to_owned(),
            slot_start_lsn: None,
            tls: None,
            acknowledgements: SourceAcknowledgementsConfig::default(),
            log_namespace: None,
        }
    }
}

impl_generate_config_from_default!(PostgresqlCdcConfig);

/// Returns `Ok(())` if `name` is a valid PostgreSQL identifier (the same rules
/// PG itself enforces for unquoted identifiers).
///
/// This is a security-critical validation: pgwire-replication's
/// `START_REPLICATION` command interpolates `replication_slot` without
/// escaping, so accepting an arbitrary string would let a malicious config
/// rewrite the replication command. Validating up-front at config load is
/// strictly preferable to runtime-escaping the value in source.rs, because
/// configuration errors surface during `vector validate` rather than at
/// startup.
fn validate_pg_identifier(name: &str, field: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    if name.len() > 63 {
        return Err(format!(
            "{field} must be at most 63 characters (PostgreSQL identifier limit)"
        ));
    }
    let mut chars = name.chars();
    let first = chars
        .next()
        .expect("name is non-empty (checked at function entry)");
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(format!(
            "{field} must start with a letter or underscore (got {first:?})"
        ));
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '_' || c == '$') {
            return Err(format!(
                "{field} may only contain ASCII letters, digits, underscores, \
                 or `$` (got {c:?})"
            ));
        }
    }
    Ok(())
}

#[async_trait::async_trait]
#[typetag::serde(name = "postgresql_cdc")]
impl SourceConfig for PostgresqlCdcConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::super::Source> {
        validate_pg_identifier(&self.replication_slot, "replication_slot")?;
        validate_pg_identifier(&self.publication_name, "publication_name")?;
        super::source::build(self.clone(), cx).await
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        // Resolve the effective namespace and register every CDC field as
        // source metadata. Under `Legacy`, each field lands at the root of
        // the `LogEvent` (the original shape); under `Vector`, each field
        // lands under `%postgresql_cdc.<field>`. We use the same
        // registration call for both modes so the schema definition stays
        // consistent and downstream schema validators know where to find
        // each field.
        let log_namespace = global_log_namespace.merge(self.log_namespace);
        let schema_definition =
            schema::Definition::default_for_namespace(&[log_namespace].into_iter().collect())
                .with_standard_vector_source_metadata()
                .with_source_metadata(
                    PostgresqlCdcConfig::NAME,
                    Some(LegacyKey::Overwrite(owned_value_path!("operation"))),
                    &owned_value_path!("operation"),
                    Kind::bytes(),
                    None,
                )
                .with_source_metadata(
                    PostgresqlCdcConfig::NAME,
                    Some(LegacyKey::Overwrite(owned_value_path!("lsn"))),
                    &owned_value_path!("lsn"),
                    Kind::bytes(),
                    None,
                )
                .with_source_metadata(
                    PostgresqlCdcConfig::NAME,
                    Some(LegacyKey::Overwrite(owned_value_path!("schema"))),
                    &owned_value_path!("schema"),
                    Kind::bytes(),
                    None,
                )
                .with_source_metadata(
                    PostgresqlCdcConfig::NAME,
                    Some(LegacyKey::Overwrite(owned_value_path!("table"))),
                    &owned_value_path!("table"),
                    Kind::bytes(),
                    None,
                )
                .with_source_metadata(
                    PostgresqlCdcConfig::NAME,
                    Some(LegacyKey::Overwrite(owned_value_path!("new"))),
                    &owned_value_path!("new"),
                    Kind::object(vrl::value::kind::Collection::any()).or_null(),
                    None,
                )
                .with_source_metadata(
                    PostgresqlCdcConfig::NAME,
                    Some(LegacyKey::Overwrite(owned_value_path!("old"))),
                    &owned_value_path!("old"),
                    Kind::object(vrl::value::kind::Collection::any()).or_null(),
                    None,
                )
                .with_source_metadata(
                    PostgresqlCdcConfig::NAME,
                    Some(LegacyKey::Overwrite(owned_value_path!("transaction_id"))),
                    &owned_value_path!("transaction_id"),
                    Kind::integer().or_null(),
                    None,
                )
                .with_source_metadata(
                    PostgresqlCdcConfig::NAME,
                    Some(LegacyKey::Overwrite(owned_value_path!(
                        "transaction_timestamp"
                    ))),
                    &owned_value_path!("transaction_timestamp"),
                    Kind::timestamp().or_null(),
                    None,
                )
                .with_source_metadata(
                    PostgresqlCdcConfig::NAME,
                    Some(LegacyKey::Overwrite(owned_value_path!("__toast_omitted"))),
                    &owned_value_path!("toast_omitted"),
                    Kind::array(vrl::value::kind::Collection::from_unknown(Kind::bytes())),
                    None,
                )
                .with_source_metadata(
                    PostgresqlCdcConfig::NAME,
                    Some(LegacyKey::Overwrite(owned_value_path!("transactional"))),
                    &owned_value_path!("transactional"),
                    Kind::boolean(),
                    None,
                )
                .with_source_metadata(
                    PostgresqlCdcConfig::NAME,
                    Some(LegacyKey::Overwrite(owned_value_path!("prefix"))),
                    &owned_value_path!("prefix"),
                    Kind::bytes(),
                    None,
                )
                .with_source_metadata(
                    PostgresqlCdcConfig::NAME,
                    Some(LegacyKey::Overwrite(owned_value_path!("content"))),
                    &owned_value_path!("content"),
                    Kind::bytes(),
                    None,
                );

        vec![SourceOutput::new_maybe_logs(
            vector_lib::config::DataType::Log,
            schema_definition,
        )]
    }

    fn can_acknowledge(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<PostgresqlCdcConfig>();
    }

    #[test]
    fn identifier_validation_accepts_normal_names() {
        for name in [
            "slot",
            "Vector_Slot",
            "slot_1",
            "slot$ext",
            "_underscore",
            "a",             // single letter
            &"a".repeat(63), // boundary
        ] {
            assert!(
                validate_pg_identifier(name, "slot").is_ok(),
                "expected {name:?} to be accepted"
            );
        }
    }

    #[test]
    fn identifier_validation_rejects_injection_attempts() {
        // SQL injection vectors via the unescaped slot identifier.
        for evil in [
            "s LOGICAL 0/0 (publication_names 'p')",
            "s; DROP SLOT",
            "s'or'1'='1",
            "s\nLOGICAL",
            "1starts_with_digit",
            "",
            &"a".repeat(64), // over 63
            "has space",
            "with-dash",
            "with.dot",
        ] {
            assert!(
                validate_pg_identifier(evil, "slot").is_err(),
                "expected {evil:?} to be rejected"
            );
        }
    }
}
