// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of [Consensus] backed by Postgres.

use std::time::Instant;

use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use bytes::Bytes;
use openssl::ssl::{SslConnector, SslFiletype, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use tokio::task::JoinHandle;
use tokio_postgres::config::SslMode;

use tokio_postgres::types::{to_sql_checked, FromSql, IsNull, ToSql, Type};
use tokio_postgres::{Client as PostgresClient, SimpleQueryMessage};

use mz_ore::task;

use crate::error::Error;
use crate::location::{Consensus, ExternalError, SeqNo, VersionedData};

// WIP: figure out if we can round-trip the `data` as bytea
//      through the Simple protocol directly, rather than
//      hex-encoding into text
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS consensus (
    shard text NOT NULL,
    sequence_number bigint NOT NULL,
    data text NOT NULL,
    PRIMARY KEY(shard, sequence_number)
);
";

impl ToSql for SeqNo {
    fn to_sql(
        &self,
        ty: &Type,
        w: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // We can only represent sequence numbers in the range [0, i64::MAX].
        let value = i64::try_from(self.0)?;
        <i64 as ToSql>::to_sql(&value, ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <i64 as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for SeqNo {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<SeqNo, Box<dyn std::error::Error + Sync + Send>> {
        let sequence_number = <i64 as FromSql>::from_sql(ty, raw)?;

        // Sanity check that the sequence number we received falls in the
        // [0, i64::MAX] range.
        let sequence_number = u64::try_from(sequence_number)?;
        Ok(SeqNo(sequence_number))
    }

    fn accepts(ty: &Type) -> bool {
        <i64 as FromSql>::accepts(ty)
    }
}

/// Configuration to connect to a Postgres backed implementation of [Consensus].
#[derive(Clone, Debug)]
pub struct PostgresConsensusConfig {
    url: String,
}

impl PostgresConsensusConfig {
    const EXTERNAL_TESTS_POSTGRES_URL: &'static str =
        "MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL";

    /// Returns a new [PostgresConsensusConfig] for use in production.
    pub async fn new(url: &str) -> Result<Self, Error> {
        Ok(PostgresConsensusConfig {
            url: url.to_string(),
        })
    }

    /// Returns a new [PostgresConsensusConfig] for use in unit tests.
    ///
    /// By default, persist tests that use external storage (like Postgres) are
    /// no-ops so that `cargo test` works on new environments without any
    /// configuration. To activate the tests for [PostgresConsensus] set the
    /// `MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL` environment variable
    /// with a valid connection url [1].
    ///
    /// [1]: https://docs.rs/tokio-postgres/latest/tokio_postgres/config/struct.Config.html#url
    pub async fn new_for_test() -> Result<Option<Self>, Error> {
        let url = match std::env::var(Self::EXTERNAL_TESTS_POSTGRES_URL) {
            Ok(url) => url,
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return Ok(None);
            }
        };

        let config = PostgresConsensusConfig::new(&url).await?;
        Ok(Some(config))
    }
}

/// Implementation of [Consensus] over a Postgres database.
#[derive(Debug)]
pub struct PostgresConsensus {
    client: PostgresClient,
    _handle: JoinHandle<()>,
}

impl PostgresConsensus {
    /// Open a Postgres [Consensus] instance with `config`, for the collection
    /// named `shard`.
    pub async fn open(config: PostgresConsensusConfig) -> Result<Self, ExternalError> {
        let pg_config: tokio_postgres::Config = config.url.parse()?;
        let tls = make_tls(&pg_config)?;
        let (mut client, conn) = tokio_postgres::connect(&config.url, tls)
            .await
            .with_context(|| format!("error connecting to postgres"))?;
        let handle = task::spawn(|| "pg_consensus_client", async move {
            if let Err(e) = conn.await {
                tracing::error!("connection error: {}", e);
                return;
            }
        });

        let tx = client.transaction().await?;
        tx.batch_execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .await?;
        let version: String = tx.query_one("SELECT version()", &[]).await?.get(0);
        // Only get the advisory lock on Postgres (but not Cockroach which doesn't
        // support this function and (we suspect) doesn't have this bug anyway). Use
        // this construction (not cockroach instead of yes postgres) to avoid
        // accidentally not executing in Postgres.
        if !version.starts_with("CockroachDB") {
            // Obtain an advisory lock before attempting to create the schema. This is
            // necessary to work around concurrency bugs in `CREATE TABLE IF NOT EXISTS`
            // in PostgreSQL.
            //
            // See: https://github.com/MaterializeInc/materialize/issues/12560
            // See: https://www.postgresql.org/message-id/CA%2BTgmoZAdYVtwBfp1FL2sMZbiHCWT4UPrzRLNnX1Nb30Ku3-gg%40mail.gmail.com
            // See: https://stackoverflow.com/a/29908840
            //
            // The lock ID was randomly generated.
            tx.batch_execute("SELECT pg_advisory_xact_lock(135664303235462630);")
                .await?;
        }
        tx.batch_execute(SCHEMA).await?;
        tx.commit().await?;
        Ok(PostgresConsensus {
            client,
            _handle: handle,
        })
    }

    /// Drops and recreates the `consensus` table in Postgres
    ///
    /// ONLY FOR TESTING
    pub async fn drop_and_recreate(&self) -> Result<(), ExternalError> {
        // this could be a TRUNCATE if we're confident the db won't reuse any state
        self.client.execute("DROP TABLE consensus", &[]).await?;
        self.client.execute(SCHEMA, &[]).await?;
        Ok(())
    }
}

// This function is copied from mz-postgres-util because of a cyclic dependency
// difficulty that we don't want to deal with now.
// TODO: Untangle that and remove this copy.
fn make_tls(config: &tokio_postgres::Config) -> Result<MakeTlsConnector, anyhow::Error> {
    let mut builder = SslConnector::builder(SslMethod::tls_client())?;
    // The mode dictates whether we verify peer certs and hostnames. By default, Postgres is
    // pretty relaxed and recommends SslMode::VerifyCa or SslMode::VerifyFull for security.
    //
    // For more details, check out Table 33.1. SSL Mode Descriptions in
    // https://postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION.
    let (verify_mode, verify_hostname) = match config.get_ssl_mode() {
        SslMode::Disable | SslMode::Prefer => (SslVerifyMode::NONE, false),
        SslMode::Require => match config.get_ssl_root_cert() {
            // If a root CA file exists, the behavior of sslmode=require will be the same as
            // that of verify-ca, meaning the server certificate is validated against the CA.
            //
            // For more details, check out the note about backwards compatibility in
            // https://postgresql.org/docs/current/libpq-ssl.html#LIBQ-SSL-CERTIFICATES.
            Some(_) => (SslVerifyMode::PEER, false),
            None => (SslVerifyMode::NONE, false),
        },
        SslMode::VerifyCa => (SslVerifyMode::PEER, false),
        SslMode::VerifyFull => (SslVerifyMode::PEER, true),
        _ => panic!("unexpected sslmode {:?}", config.get_ssl_mode()),
    };

    // Configure peer verification
    builder.set_verify(verify_mode);

    // Configure certificates
    match (config.get_ssl_cert(), config.get_ssl_key()) {
        (Some(ssl_cert), Some(ssl_key)) => {
            builder.set_certificate_file(ssl_cert, SslFiletype::PEM)?;
            builder.set_private_key_file(ssl_key, SslFiletype::PEM)?;
        }
        (None, Some(_)) => bail!("must provide both sslcert and sslkey, but only provided sslkey"),
        (Some(_), None) => bail!("must provide both sslcert and sslkey, but only provided sslcert"),
        _ => {}
    }
    if let Some(ssl_root_cert) = config.get_ssl_root_cert() {
        builder.set_ca_file(ssl_root_cert)?
    }

    let mut tls_connector = MakeTlsConnector::new(builder.build());

    // Configure hostname verification
    match (verify_mode, verify_hostname) {
        (SslVerifyMode::PEER, false) => tls_connector.set_callback(|connect, _| {
            connect.set_verify_hostname(false);
            Ok(())
        }),
        _ => {}
    }

    Ok(tls_connector)
}

#[async_trait]
impl Consensus for PostgresConsensus {
    async fn head(
        &self,
        _deadline: Instant,
        key: &str,
    ) -> Result<Option<VersionedData>, ExternalError> {
        // TODO: properly use the deadline argument.

        let q = "SELECT sequence_number, data FROM consensus
             WHERE shard = $1 ORDER BY sequence_number DESC LIMIT 1";
        let row = self.client.query_opt(&*q, &[&key]).await?;
        let row = match row {
            None => return Ok(None),
            Some(row) => row,
        };

        let seqno: SeqNo = row.try_get("sequence_number")?;

        let data: &str = row.try_get("data")?;
        Ok(Some(VersionedData {
            seqno,
            data: Bytes::from(hex::decode(data).expect("TODO")),
        }))
    }

    async fn compare_and_set(
        &self,
        deadline: Instant,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<Result<(), Option<VersionedData>>, ExternalError> {
        // TODO: properly use the deadline argument.

        if let Some(expected) = expected {
            if new.seqno <= expected {
                return Err(Error::from(
                        format!("new seqno must be strictly greater than expected. Got new: {:?} expected: {:?}",
                                 new.seqno, expected)).into());
            }
        }

        let rows_upserted = if let Some(expected) = expected {
            // Only insert the new row if:
            // - the expected sequence number is currently the greatest sequence number in `consensus`
            // Otherwise:
            // - return the greatest sequence number and its data
            //
            // This query has several subtle tricks to both execute within a single network
            // round-trip and avoid serializability conflict retries.
            // - It selects the greatest sequence number with a FOR UPDATE locking clause. This
            //   causes the statement to block until the transaction has exclusive access to the
            //   greatest sequence number for a given shard, and will always see the most recent
            //   write when it is granted the lock.
            //
            //   Note: this behavior only applies to Cockroach[0] and not Postgres! Postgres under
            //         serializable isolation will throw a serialized access error when its
            //         FOR UPDATE statement is unblocked if the greatest sequence number has
            //         changed (which is almost always true if it was initially blocked).
            //
            // - It conditionally INSERTs a new row into consensus iff the greatest sequence
            //   number is of the expected value, and then uses INSERT ... RETURNING to indicate
            //   whether the insert succeeded
            //
            // - The SELECT statement with the greatest sequence number (and its data) is unioned
            //   with the results of the conditional INSERT, allowing the client to read both
            //   the latest persisted value and whether its write succeeded.
            //
            // - The whole statement is executed inside of a single-batch transaction, using the
            //   Postgres Simple Query protocol [1]. This avoids round-trips for `BEGIN` and
            //   `COMMIT` statements, at the cost of losing prepared statements and needing to
            //   to use string interpolation to build the query. This is means the query is
            //   NOT injection safe were it ever to include data controlled by the user.
            //
            //   [0]: https://www.cockroachlabs.com/docs/v21.2/select-for-update
            //   [1]: https://www.cockroachlabs.com/docs/v21.2/transactions#batched-statements
            let conditional_insertion = format!(
                r#"
                    WITH
                      greatest_seqno AS (
                        SELECT sequence_number, data FROM consensus WHERE shard = '{shard}' ORDER BY sequence_number DESC LIMIT 1 FOR UPDATE
                      ),
                      inserted AS (
                        INSERT INTO consensus (shard, sequence_number, data)
                        SELECT '{shard}', {new_seqno}, '{data}'
                        WHERE EXISTS (SELECT 1 FROM greatest_seqno WHERE sequence_number = '{expected_seqno}')
                        ON CONFLICT DO NOTHING
                        RETURNING sequence_number
                      )
                    SELECT true as was_inserted, sequence_number, NULL as data FROM inserted
                    UNION ALL
                    SELECT false as was_inserted, sequence_number, data FROM greatest_seqno
            "#,
                shard = &key,
                expected_seqno = expected.0,
                new_seqno = new.seqno.0,
                data = hex::encode(&new.data),
            );

            let insert = self
                .client
                .simple_query(&format!("BEGIN; {}; COMMIT", conditional_insertion))
                .await;

            let mut any_inserted = false;
            match insert {
                Ok(query_messages) => {
                    for (i, message) in (&query_messages).iter().enumerate() {
                        if i == 1 {
                            match message {
                                SimpleQueryMessage::Row(r) => {
                                    let was_inserted = r.get("was_inserted").expect(
                                        "non-erroring query must have returned was_inserted column",
                                    ) == "t";

                                    if was_inserted {
                                        any_inserted = true;
                                        break;
                                    }

                                    let seqno : u64 = r.get("sequence_number").expect("non-erroring query must have returned greatest known sequence number").parse().expect("sequence number must be u64");
                                    let data = r.get("data").expect("non-erroring query must have returned data if was_inserted is false");

                                    return Ok(Err(Some(VersionedData {
                                        seqno: SeqNo(seqno),
                                        data: Bytes::from(hex::decode(data).expect("TODO")),
                                    })));
                                }
                                _ => {}
                            }
                        }
                    }
                }
                Err(e) => {
                    //  WIP: A failed transaction could cause other queries from this client to fail
                    //       until the transaction is aborted. Use a connection pool so we can isolate
                    //       the transaction from other queries.
                    self.client.batch_execute(&"ABORT").await?;
                    return Err(e.into());
                }
            }

            if any_inserted {
                1
            } else {
                0
            }
        } else {
            // Insert the new row as long as no other row exists for the same shard.
            let q = "INSERT INTO consensus SELECT $1, $2, $3 WHERE
                     NOT EXISTS (
                         SELECT * FROM consensus WHERE shard = $1
                     )
                     ON CONFLICT DO NOTHING";
            self.client
                .execute(&*q, &[&key, &new.seqno, &hex::encode(&new.data)])
                .await?
        };

        if rows_upserted == 1 {
            Ok(Ok(()))
        } else {
            // It's safe to call head in a subsequent transaction rather than doing
            // so directly in the same transaction because, once a given (seqno, data)
            // pair exists for our shard, we enforce the invariants that
            // 1. Our shard will always have _some_ data mapped to it.
            // 2. All operations that modify the (seqno, data) can only increase
            //    the sequence number.
            let current = self.head(deadline, key).await?;
            Ok(Err(current))
        }
    }

    async fn scan(
        &self,
        _deadline: Instant,
        key: &str,
        from: SeqNo,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        // TODO: properly use the deadline argument.

        let q = "SELECT sequence_number, data FROM consensus
             WHERE shard = $1 AND sequence_number >= $2
             ORDER BY sequence_number";
        let rows = self.client.query(&*q, &[&key, &from]).await?;
        let mut results = vec![];

        for row in rows {
            let seqno: SeqNo = row.try_get("sequence_number")?;
            let data: Vec<u8> = row.try_get("data")?;
            results.push(VersionedData {
                seqno,
                data: Bytes::from(data),
            });
        }

        if results.is_empty() {
            Err(ExternalError::from(anyhow!(
                "sequence number lower bound too high for scan: {:?}",
                from
            )))
        } else {
            Ok(results)
        }
    }

    async fn truncate(
        &self,
        deadline: Instant,
        key: &str,
        seqno: SeqNo,
    ) -> Result<(), ExternalError> {
        let q = "DELETE FROM consensus
                WHERE shard = $1 AND sequence_number < $2 AND
                EXISTS(
                    SELECT * FROM consensus WHERE shard = $1 AND sequence_number >= $2
                )";

        let result = { self.client.execute(&*q, &[&key, &seqno]).await? };
        if result == 0 {
            // We weren't able to successfully truncate any rows inspect head to
            // determine whether the request was valid and there were no records in
            // the provided range, or the request was invalid because it would have
            // also deleted head.

            // It's safe to call head in a subsequent transaction rather than doing
            // so directly in the same transaction because, once a given (seqno, data)
            // pair exists for our shard, we enforce the invariants that
            // 1. Our shard will always have _some_ data mapped to it.
            // 2. All operations that modify the (seqno, data) can only increase
            //    the sequence number.
            let current = self.head(deadline, key).await?;
            if current.map_or(true, |data| data.seqno < seqno) {
                return Err(ExternalError::from(anyhow!(
                    "upper bound too high for truncate: {:?}",
                    seqno
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::location::tests::consensus_impl_test;
    use std::time::Duration;
    use tracing::info;
    use uuid::Uuid;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn postgres_consensus() -> Result<(), ExternalError> {
        let config = match PostgresConsensusConfig::new_for_test().await? {
            Some(config) => config,
            None => {
                info!(
                    "{} env not set: skipping test that uses external service",
                    PostgresConsensusConfig::EXTERNAL_TESTS_POSTGRES_URL
                );
                return Ok(());
            }
        };

        consensus_impl_test(|| PostgresConsensus::open(config.clone())).await?;

        // and now verify the implementation-specific `drop_and_recreate` works as intended
        let consensus = PostgresConsensus::open(config.clone()).await?;
        let deadline = Instant::now() + Duration::from_secs(5);
        let key = Uuid::new_v4().to_string();
        let state = VersionedData {
            seqno: SeqNo(5),
            data: Bytes::from("abc"),
        };

        assert_eq!(
            consensus
                .compare_and_set(deadline, &key, None, state.clone())
                .await,
            Ok(Ok(()))
        );

        assert_eq!(
            consensus.head(deadline, &key).await,
            Ok(Some(state.clone()))
        );

        consensus.drop_and_recreate().await?;

        assert_eq!(consensus.head(deadline, &key).await, Ok(None));

        Ok(())
    }
}
