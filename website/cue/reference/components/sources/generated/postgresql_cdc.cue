package metadata

generated: components: sources: postgresql_cdc: configuration: {
	acknowledgements: {
		deprecated: true
		description: """
			End-to-end acknowledgement configuration.

			When acknowledgements are enabled (the default for this source) the
			confirmed-flush LSN is only advanced after every downstream sink has
			acknowledged the events produced by the corresponding transaction.
			"""
		required: false
		type: object: options: enabled: {
			description: "Whether or not end-to-end acknowledgements are enabled for this source."
			required:    false
			type: bool: {}
		}
	}
	connection_string: {
		description: """
			libpq-style connection URI for the PostgreSQL server.

			Must NOT include replication-specific parameters such as `replication`
			or `database=replication`; the source sets those internally on the
			replication connection.
			"""
		required: true
		type: string: examples: ["postgres://vector:password@db.example.com:5432/app"]
	}
	publication_name: {
		description: """
			Name of an existing publication to subscribe to. The publication
			determines which tables and DML operations are replicated.
			"""
		required: true
		type: string: examples: ["vector_pub"]
	}
	replication_slot: {
		description: """
			Name of an existing logical replication slot that uses the `pgoutput`
			output plugin. The slot must be created out-of-band (e.g. via
			`SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput')`) —
			the source will not create or drop it.
			"""
		required: true
		type: string: examples: ["vector_slot"]
	}
	slot_start_lsn: {
		description: """
			Optional LSN to start streaming from, in `X/Y` hex format.

			If unset, the source passes `0/0` to `START_REPLICATION`, which
			causes PostgreSQL to resume from the slot's `confirmed_flush_lsn`
			automatically. Override only for advanced replay scenarios.
			"""
		required: false
		type: string: examples: ["16/B374D848"]
	}
	tls: {
		description: """
			TLS configuration for the connection. If unset, the source connects
			without TLS.
			"""
		required: false
		type: object: options: {
			ca_file: {
				description: """
					Absolute path to a PEM-encoded CA certificate file used to verify the
					Postgres server certificate.
					"""
				required: true
				type: string: examples: ["/etc/ssl/certs/ca.pem"]
			}
			verify_hostname: {
				description: """
					Whether to verify the server hostname against the certificate.

					Set to `false` to verify the certificate chain only (equivalent to
					`sslmode=verify-ca`). Default is `true` (`sslmode=verify-full`).
					"""
				required: false
				type: bool: default: true
			}
		}
	}
}
