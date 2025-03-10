// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! AST types specific to CREATE/ALTER variants of [crate::ast::Statement]
//! (commonly referred to as Data Definition Language, or DDL)

use std::fmt;
use std::path::PathBuf;

use crate::ast::display::{self, AstDisplay, AstFormatter};
use crate::ast::{AstInfo, Expr, Ident, UnresolvedObjectName, WithOption, WithOptionValue};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Schema {
    File(PathBuf),
    Inline(String),
}

impl AstDisplay for Schema {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::File(path) => {
                f.write_str("SCHEMA FILE '");
                f.write_node(&display::escape_single_quote_string(
                    &path.display().to_string(),
                ));
                f.write_str("'");
            }
            Self::Inline(inner) => {
                f.write_str("SCHEMA '");
                f.write_node(&display::escape_single_quote_string(inner));
                f.write_str("'");
            }
        }
    }
}
impl_display!(Schema);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AvroSchemaOptionName {
    /// The `CONFLUENT WIRE FORMAT [=] <bool>` option.
    ConfluentWireFormat,
}

impl AstDisplay for AvroSchemaOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            AvroSchemaOptionName::ConfluentWireFormat => f.write_str("CONFLUENT WIRE FORMAT"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AvroSchemaOption<T: AstInfo> {
    pub name: AvroSchemaOptionName,
    pub value: Option<WithOptionValue<T>>,
}

impl<T: AstInfo> AstDisplay for AvroSchemaOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        if let Some(v) = &self.value {
            f.write_str(" = ");
            f.write_node(v);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AvroSchema<T: AstInfo> {
    Csr {
        csr_connection: CsrConnectionAvro<T>,
    },
    InlineSchema {
        schema: Schema,
        with_options: Vec<AvroSchemaOption<T>>,
    },
}

impl<T: AstInfo> AstDisplay for AvroSchema<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Csr { csr_connection } => {
                f.write_node(csr_connection);
            }
            Self::InlineSchema {
                schema,
                with_options,
            } => {
                f.write_str("USING ");
                schema.fmt(f);
                if !with_options.is_empty() {
                    f.write_str(" WITH (");
                    f.write_node(&display::comma_separated(with_options));
                    f.write_str(")");
                }
            }
        }
    }
}
impl_display_t!(AvroSchema);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ProtobufSchema<T: AstInfo> {
    Csr {
        csr_connection: CsrConnectionProto<T>,
    },
    InlineSchema {
        message_name: String,
        schema: Schema,
    },
}

impl<T: AstInfo> AstDisplay for ProtobufSchema<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Csr { csr_connection } => {
                f.write_node(csr_connection);
            }
            Self::InlineSchema {
                message_name,
                schema,
            } => {
                f.write_str("MESSAGE '");
                f.write_node(&display::escape_single_quote_string(message_name));
                f.write_str("' USING ");
                f.write_str(schema);
            }
        }
    }
}
impl_display_t!(ProtobufSchema);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CsrConnection<T: AstInfo> {
    Inline { url: String },
    Reference { connection: T::ObjectName },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrConnectionAvro<T: AstInfo> {
    pub connection: CsrConnection<T>,
    pub seed: Option<CsrSeed>,
    pub with_options: Vec<WithOption<T>>,
}

impl<T: AstInfo> AstDisplay for CsrConnectionAvro<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("USING CONFLUENT SCHEMA REGISTRY ");
        match &self.connection {
            CsrConnection::Inline { url: uri, .. } => {
                f.write_str("'");
                f.write_node(&display::escape_single_quote_string(uri));
                f.write_str("'");
            }
            CsrConnection::Reference { connection, .. } => {
                f.write_str("CONNECTION ");
                f.write_node(connection);
            }
        }
        if let Some(seed) = &self.seed {
            f.write_str(" ");
            f.write_node(seed);
        }
        if !&self.with_options.is_empty() {
            f.write_str(" WITH (");
            f.write_node(&display::comma_separated(&self.with_options));
            f.write_str(")");
        }
    }
}
impl_display_t!(CsrConnectionAvro);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrConnectionProto<T: AstInfo> {
    pub connection: CsrConnection<T>,
    pub seed: Option<CsrSeedCompiledOrLegacy>,
    pub with_options: Vec<WithOption<T>>,
}

impl<T: AstInfo> AstDisplay for CsrConnectionProto<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("USING CONFLUENT SCHEMA REGISTRY ");
        match &self.connection {
            CsrConnection::Inline { url: uri, .. } => {
                f.write_str("'");
                f.write_node(&display::escape_single_quote_string(uri));
                f.write_str("'");
            }
            CsrConnection::Reference { connection, .. } => {
                f.write_str("CONNECTION ");
                f.write_node(connection);
            }
        }

        if let Some(seed) = &self.seed {
            f.write_str(" ");
            f.write_node(seed);
        }

        if !&self.with_options.is_empty() {
            f.write_str(" WITH (");
            f.write_node(&display::comma_separated(&self.with_options));
            f.write_str(")");
        }
    }
}
impl_display_t!(CsrConnectionProto);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrSeed {
    pub key_schema: Option<String>,
    pub value_schema: String,
}

impl AstDisplay for CsrSeed {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SEED");
        if let Some(key_schema) = &self.key_schema {
            f.write_str(" KEY SCHEMA '");
            f.write_node(&display::escape_single_quote_string(key_schema));
            f.write_str("'");
        }
        f.write_str(" VALUE SCHEMA '");
        f.write_node(&display::escape_single_quote_string(&self.value_schema));
        f.write_str("'");
    }
}
impl_display!(CsrSeed);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CsrSeedCompiledOrLegacy {
    Compiled(CsrSeedCompiled),
    // Starting with version 0.9.13, Legacy should only be found when reading
    // from the catalog and should be transformed during migration.
    Legacy(CsrSeed),
}
impl AstDisplay for CsrSeedCompiledOrLegacy {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            CsrSeedCompiledOrLegacy::Compiled(c) => f.write_node(c),
            CsrSeedCompiledOrLegacy::Legacy(l) => f.write_node(l),
        }
    }
}
impl_display!(CsrSeedCompiledOrLegacy);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrSeedCompiled {
    pub key: Option<CsrSeedCompiledEncoding>,
    pub value: CsrSeedCompiledEncoding,
}
impl AstDisplay for CsrSeedCompiled {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SEED COMPILED");
        if let Some(key) = &self.key {
            f.write_str(" KEY ");
            f.write_node(key);
        }
        f.write_str(" VALUE ");
        f.write_node(&self.value);
    }
}
impl_display!(CsrSeedCompiled);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrSeedCompiledEncoding {
    // Hex encoded string.
    pub schema: String,
    pub message_name: String,
}
impl AstDisplay for CsrSeedCompiledEncoding {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(" SCHEMA '");
        f.write_str(&display::escape_single_quote_string(&self.schema));
        f.write_str("' MESSAGE '");
        f.write_str(&self.message_name);
        f.write_str("'");
    }
}
impl_display!(CsrSeedCompiledEncoding);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CreateSourceFormat<T: AstInfo> {
    None,
    /// `CREATE SOURCE .. FORMAT`
    Bare(Format<T>),
    /// `CREATE SOURCE .. KEY FORMAT .. VALUE FORMAT`
    ///
    /// Also the destination for the legacy `ENVELOPE UPSERT FORMAT ...`
    KeyValue {
        key: Format<T>,
        value: Format<T>,
    },
}

impl<T: AstInfo> AstDisplay for CreateSourceFormat<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            CreateSourceFormat::None => {}
            CreateSourceFormat::Bare(format) => {
                f.write_str(" FORMAT ");
                f.write_node(format)
            }
            CreateSourceFormat::KeyValue { key, value } => {
                f.write_str(" KEY FORMAT ");
                f.write_node(key);
                f.write_str(" VALUE FORMAT ");
                f.write_node(value);
            }
        }
    }
}
impl_display_t!(CreateSourceFormat);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Format<T: AstInfo> {
    Bytes,
    Avro(AvroSchema<T>),
    Protobuf(ProtobufSchema<T>),
    Regex(String),
    Csv {
        columns: CsvColumns,
        delimiter: char,
    },
    Json,
    Text,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CsvColumns {
    /// `WITH count COLUMNS`
    Count(usize),
    /// `WITH HEADER (ident, ...)?`: `names` is empty if there are no names specified
    Header { names: Vec<Ident> },
}

impl AstDisplay for CsvColumns {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            CsvColumns::Count(n) => {
                f.write_str(n);
                f.write_str(" COLUMNS")
            }
            CsvColumns::Header { names } => {
                f.write_str("HEADER");
                if !names.is_empty() {
                    f.write_str(" (");
                    f.write_node(&display::comma_separated(&names));
                    f.write_str(")");
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SourceIncludeMetadataType {
    Key,
    Timestamp,
    Partition,
    Topic,
    Offset,
    Headers,
}

impl AstDisplay for SourceIncludeMetadataType {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            SourceIncludeMetadataType::Key => f.write_str("KEY"),
            SourceIncludeMetadataType::Timestamp => f.write_str("TIMESTAMP"),
            SourceIncludeMetadataType::Partition => f.write_str("PARTITION"),
            SourceIncludeMetadataType::Topic => f.write_str("TOPIC"),
            SourceIncludeMetadataType::Offset => f.write_str("OFFSET"),
            SourceIncludeMetadataType::Headers => f.write_str("HEADERS"),
        }
    }
}
impl_display!(SourceIncludeMetadataType);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceIncludeMetadata {
    pub ty: SourceIncludeMetadataType,
    pub alias: Option<Ident>,
}

impl AstDisplay for SourceIncludeMetadata {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.ty);
        if let Some(alias) = &self.alias {
            f.write_str(" AS ");
            f.write_node(alias);
        }
    }
}
impl_display!(SourceIncludeMetadata);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Envelope<T: AstInfo> {
    None,
    Debezium(DbzMode<T>),
    Upsert,
    CdcV2,
}

impl<T: AstInfo> AstDisplay for Envelope<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::None => {
                // this is unreachable as long as the default is None, but include it in case we ever change that
                f.write_str("NONE");
            }
            Self::Debezium(mode) => {
                f.write_str("DEBEZIUM");
                f.write_node(mode);
            }
            Self::Upsert => {
                f.write_str("UPSERT");
            }
            Self::CdcV2 => {
                f.write_str("MATERIALIZE");
            }
        }
    }
}
impl_display_t!(Envelope);

impl<T: AstInfo> AstDisplay for Format<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Bytes => f.write_str("BYTES"),
            Self::Avro(inner) => {
                f.write_str("AVRO ");
                f.write_node(inner);
            }
            Self::Protobuf(inner) => {
                f.write_str("PROTOBUF ");
                f.write_node(inner);
            }
            Self::Regex(regex) => {
                f.write_str("REGEX '");
                f.write_node(&display::escape_single_quote_string(regex));
                f.write_str("'");
            }
            Self::Csv { columns, delimiter } => {
                f.write_str("CSV WITH ");
                f.write_node(columns);

                if *delimiter != ',' {
                    f.write_str(" DELIMITED BY '");
                    f.write_node(&display::escape_single_quote_string(&delimiter.to_string()));
                    f.write_str("'");
                }
            }
            Self::Json => f.write_str("JSON"),
            Self::Text => f.write_str("TEXT"),
        }
    }
}
impl_display_t!(Format);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Compression {
    Gzip,
    None,
}

impl AstDisplay for Compression {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Gzip => f.write_str("GZIP"),
            Self::None => f.write_str("NONE"),
        }
    }
}
impl_display!(Compression);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DbzMode<T: AstInfo> {
    /// `ENVELOPE DEBEZIUM [TRANSACTION METADATA (...)]`
    Plain {
        tx_metadata: Vec<DbzTxMetadataOption<T>>,
    },
    /// `ENVELOPE DEBEZIUM UPSERT`
    Upsert,
}

impl<T: AstInfo> AstDisplay for DbzMode<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Plain { tx_metadata } => {
                if !tx_metadata.is_empty() {
                    f.write_str(" (TRANSACTION METADATA (");
                    f.write_node(&display::comma_separated(tx_metadata));
                    f.write_str("))");
                }
            }
            Self::Upsert => f.write_str(" UPSERT"),
        }
    }
}
impl_display_t!(DbzMode);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DbzTxMetadataOption<T: AstInfo> {
    Source(T::ObjectName),
    Collection(WithOptionValue<T>),
}
impl<T: AstInfo> AstDisplay for DbzTxMetadataOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            DbzTxMetadataOption::Source(source) => {
                f.write_str("SOURCE ");
                f.write_node(source);
            }
            DbzTxMetadataOption::Collection(collection) => {
                f.write_str("COLLECTION ");
                f.write_node(collection);
            }
        }
    }
}
impl_display_t!(DbzTxMetadataOption);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KafkaConnectionOptionName {
    Broker,
    Brokers,
    SslKey,
    SslKeyPassword,
    SslCertificate,
    SslCertificateAuthority,
    SaslMechanisms,
    SaslUsername,
    SaslPassword,
}

impl AstDisplay for KafkaConnectionOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            KafkaConnectionOptionName::Broker => "BROKER",
            KafkaConnectionOptionName::Brokers => "BROKERS",
            KafkaConnectionOptionName::SslKey => "SSL KEY",
            KafkaConnectionOptionName::SslKeyPassword => "SSL KEY PASSWORD",
            KafkaConnectionOptionName::SslCertificate => "SSL CERTIFICATE",
            KafkaConnectionOptionName::SslCertificateAuthority => "SSL CERTIFICATE AUTHORITY",
            KafkaConnectionOptionName::SaslMechanisms => "SASL MECHANISMS",
            KafkaConnectionOptionName::SaslUsername => "SASL USERNAME",
            KafkaConnectionOptionName::SaslPassword => "SASL PASSWORD",
        })
    }
}
impl_display!(KafkaConnectionOptionName);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// An option in a `CREATE CONNECTION...KAFKA`.
pub struct KafkaConnectionOption<T: AstInfo> {
    pub name: KafkaConnectionOptionName,
    pub value: Option<WithOptionValue<T>>,
}

impl<T: AstInfo> AstDisplay for KafkaConnectionOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        if let Some(v) = &self.value {
            f.write_str(" = ");
            f.write_node(v);
        }
    }
}
impl_display_t!(KafkaConnectionOption);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CreateConnection<T: AstInfo> {
    Kafka {
        with_options: Vec<KafkaConnectionOption<T>>,
    },
    Csr {
        url: String,
        with_options: Vec<WithOption<T>>,
    },
}

impl<T: AstInfo> AstDisplay for CreateConnection<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Kafka { with_options } => {
                f.write_str("KAFKA ");
                f.write_node(&display::comma_separated(&with_options));
            }
            Self::Csr {
                url: registry,
                with_options,
            } => {
                f.write_str("CONFLUENT SCHEMA REGISTRY '");
                f.write_node(&display::escape_single_quote_string(registry));
                f.write_str("'");
                if with_options.len() > 0 {
                    f.write_str(" WITH (");
                    f.write_node(&display::comma_separated(&with_options));
                    f.write_str(")");
                }
            }
        }
    }
}
impl_display_t!(CreateConnection);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KafkaConnection<T: AstInfo> {
    Inline { broker: String },
    Reference { connection: T::ObjectName },
}

impl<T: AstInfo> AstDisplay for KafkaConnection<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            KafkaConnection::Inline { broker } => {
                f.write_str("BROKER '");
                f.write_node(&display::escape_single_quote_string(broker));
                f.write_str("'");
            }
            KafkaConnection::Reference { connection, .. } => {
                f.write_str("CONNECTION ");
                f.write_node(connection);
            }
        }
    }
}
impl_display_t!(KafkaConnection);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KafkaSourceConnection<T: AstInfo> {
    pub connection: KafkaConnection<T>,
    pub topic: String,
    pub key: Option<Vec<Ident>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CreateSourceConnection<T: AstInfo> {
    Kafka(KafkaSourceConnection<T>),
    Kinesis {
        arn: String,
    },
    S3 {
        /// The arguments to `DISCOVER OBJECTS USING`: `BUCKET SCAN` or `SQS NOTIFICATIONS`
        key_sources: Vec<S3KeySource>,
        /// The argument to the MATCHING clause: `MATCHING 'a/**/*.json'`
        pattern: Option<String>,
        compression: Compression,
    },
    Postgres {
        /// The postgres connection string
        conn: String,
        /// The name of the publication to sync
        publication: String,
        /// Hex encoded string of binary serialization of `dataflow_types::PostgresSourceDetails`
        details: Option<String>,
    },
    PubNub {
        /// PubNub's subscribe key
        subscribe_key: String,
        /// The PubNub channel to subscribe to
        channel: String,
    },
}

impl<T: AstInfo> AstDisplay for CreateSourceConnection<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            CreateSourceConnection::Kafka(KafkaSourceConnection {
                connection,
                topic,
                key,
            }) => {
                f.write_str("KAFKA ");
                f.write_node(connection);
                f.write_str(" TOPIC '");
                f.write_node(&display::escape_single_quote_string(topic));
                f.write_str("'");
                if let Some(key) = key.as_ref() {
                    f.write_str(" KEY (");
                    f.write_node(&display::comma_separated(&key));
                    f.write_str(")");
                }
            }
            CreateSourceConnection::Kinesis { arn } => {
                f.write_str("KINESIS ARN '");
                f.write_node(&display::escape_single_quote_string(arn));
                f.write_str("'");
            }
            CreateSourceConnection::S3 {
                key_sources,
                pattern,
                compression,
            } => {
                f.write_str("S3 DISCOVER OBJECTS");
                if let Some(pattern) = pattern {
                    f.write_str(" MATCHING '");
                    f.write_str(&display::escape_single_quote_string(pattern));
                    f.write_str("'");
                }
                f.write_str(" USING");
                f.write_node(&display::comma_separated(key_sources));
                f.write_str(" COMPRESSION ");
                f.write_node(compression);
            }
            CreateSourceConnection::Postgres {
                conn,
                publication,
                details,
            } => {
                f.write_str("POSTGRES CONNECTION '");
                f.write_str(&display::escape_single_quote_string(conn));
                f.write_str("' PUBLICATION '");
                f.write_str(&display::escape_single_quote_string(publication));
                if let Some(details) = details {
                    f.write_str("' DETAILS '");
                    f.write_str(&display::escape_single_quote_string(details));
                }
                f.write_str("'");
            }
            CreateSourceConnection::PubNub {
                subscribe_key,
                channel,
            } => {
                f.write_str("PUBNUB SUBSCRIBE KEY '");
                f.write_str(&display::escape_single_quote_string(subscribe_key));
                f.write_str("' CHANNEL '");
                f.write_str(&display::escape_single_quote_string(channel));
                f.write_str("'");
            }
        }
    }
}
impl_display_t!(CreateSourceConnection);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CreateSinkConnection<T: AstInfo> {
    Kafka {
        broker: String,
        topic: String,
        key: Option<KafkaSinkKey>,
        consistency: Option<KafkaConsistency<T>>,
    },
    Persist,
}

impl<T: AstInfo> AstDisplay for CreateSinkConnection<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            CreateSinkConnection::Kafka {
                broker,
                topic,
                key,
                consistency,
            } => {
                f.write_str("KAFKA BROKER '");
                f.write_node(&display::escape_single_quote_string(broker));
                f.write_str("'");
                f.write_str(" TOPIC '");
                f.write_node(&display::escape_single_quote_string(topic));
                f.write_str("'");
                if let Some(key) = key.as_ref() {
                    f.write_node(key);
                }
                if let Some(consistency) = consistency.as_ref() {
                    f.write_node(consistency);
                }
            }
            CreateSinkConnection::Persist => {
                f.write_str("PERSIST");
            }
        }
    }
}
impl_display_t!(CreateSinkConnection);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KafkaConsistency<T: AstInfo> {
    pub topic: String,
    pub topic_format: Option<Format<T>>,
}

impl<T: AstInfo> AstDisplay for KafkaConsistency<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(" CONSISTENCY (TOPIC '");
        f.write_node(&display::escape_single_quote_string(&self.topic));
        f.write_str("'");

        if let Some(format) = self.topic_format.as_ref() {
            f.write_str(" FORMAT ");
            f.write_node(format);
        }
        f.write_str(")");
    }
}
impl_display_t!(KafkaConsistency);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KafkaSinkKey {
    pub key_columns: Vec<Ident>,
    pub not_enforced: bool,
}

impl AstDisplay for KafkaSinkKey {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(" KEY (");
        f.write_node(&display::comma_separated(&self.key_columns));
        f.write_str(")");
        if self.not_enforced {
            f.write_str(" NOT ENFORCED");
        }
    }
}

/// Information about upstream Postgres tables used for replication sources
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PgTable<T: AstInfo> {
    /// The name of the table to sync
    pub name: UnresolvedObjectName,
    /// The name for the table in Materialize
    pub alias: T::ObjectName,
    /// The expected column schema of the synced table
    pub columns: Vec<ColumnDef<T>>,
}

impl<T: AstInfo> AstDisplay for PgTable<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        f.write_str(" AS ");
        f.write_str(self.alias.to_ast_string());
        if !self.columns.is_empty() {
            f.write_str(" (");
            f.write_node(&display::comma_separated(&self.columns));
            f.write_str(")");
        }
    }
}
impl_display_t!(PgTable);

/// The key sources specified in the S3 source's `DISCOVER OBJECTS` clause.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum S3KeySource {
    /// `SCAN BUCKET '<bucket>'`
    Scan { bucket: String },
    /// `SQS NOTIFICATIONS '<queue-name>'`
    SqsNotifications { queue: String },
}

impl AstDisplay for S3KeySource {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            S3KeySource::Scan { bucket } => {
                f.write_str(" BUCKET SCAN '");
                f.write_str(&display::escape_single_quote_string(bucket));
                f.write_str("'");
            }
            S3KeySource::SqsNotifications { queue } => {
                f.write_str(" SQS NOTIFICATIONS '");
                f.write_str(&display::escape_single_quote_string(queue));
                f.write_str("'");
            }
        }
    }
}
impl_display!(S3KeySource);

/// A table-level constraint, specified in a `CREATE TABLE` or an
/// `ALTER TABLE ADD <constraint>` statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableConstraint<T: AstInfo> {
    /// `[ CONSTRAINT <name> ] { PRIMARY KEY | UNIQUE } (<columns>)`
    Unique {
        name: Option<Ident>,
        columns: Vec<Ident>,
        /// Whether this is a `PRIMARY KEY` or just a `UNIQUE` constraint
        is_primary: bool,
    },
    /// A referential integrity constraint (`[ CONSTRAINT <name> ] FOREIGN KEY (<columns>)
    /// REFERENCES <foreign_table> (<referred_columns>)`)
    ForeignKey {
        name: Option<Ident>,
        columns: Vec<Ident>,
        foreign_table: T::ObjectName,
        referred_columns: Vec<Ident>,
    },
    /// `[ CONSTRAINT <name> ] CHECK (<expr>)`
    Check {
        name: Option<Ident>,
        expr: Box<Expr<T>>,
    },
}

impl<T: AstInfo> AstDisplay for TableConstraint<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            TableConstraint::Unique {
                name,
                columns,
                is_primary,
            } => {
                f.write_node(&display_constraint_name(name));
                if *is_primary {
                    f.write_str("PRIMARY KEY ");
                } else {
                    f.write_str("UNIQUE ");
                }
                f.write_str("(");
                f.write_node(&display::comma_separated(columns));
                f.write_str(")");
            }
            TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
            } => {
                f.write_node(&display_constraint_name(name));
                f.write_str("FOREIGN KEY (");
                f.write_node(&display::comma_separated(columns));
                f.write_str(") REFERENCES ");
                f.write_node(foreign_table);
                f.write_str("(");
                f.write_node(&display::comma_separated(referred_columns));
                f.write_str(")");
            }
            TableConstraint::Check { name, expr } => {
                f.write_node(&display_constraint_name(name));
                f.write_str("CHECK (");
                f.write_node(&expr);
                f.write_str(")");
            }
        }
    }
}
impl_display_t!(TableConstraint);

/// A key constraint, specified in a `CREATE SOURCE`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KeyConstraint {
    // PRIMARY KEY (<columns>) NOT ENFORCED
    PrimaryKeyNotEnforced { columns: Vec<Ident> },
}

impl AstDisplay for KeyConstraint {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            KeyConstraint::PrimaryKeyNotEnforced { columns } => {
                f.write_str("PRIMARY KEY ");
                f.write_str("(");
                f.write_node(&display::comma_separated(columns));
                f.write_str(") ");
                f.write_str("NOT ENFORCED");
            }
        }
    }
}
impl_display!(KeyConstraint);

/// SQL column definition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDef<T: AstInfo> {
    pub name: Ident,
    pub data_type: T::DataType,
    pub collation: Option<UnresolvedObjectName>,
    pub options: Vec<ColumnOptionDef<T>>,
}

impl<T: AstInfo> AstDisplay for ColumnDef<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        f.write_str(" ");
        f.write_node(&self.data_type);
        for option in &self.options {
            f.write_str(" ");
            f.write_node(option);
        }
    }
}
impl_display_t!(ColumnDef);

/// An optionally-named `ColumnOption`: `[ CONSTRAINT <name> ] <column-option>`.
///
/// Note that implementations are substantially more permissive than the ANSI
/// specification on what order column options can be presented in, and whether
/// they are allowed to be named. The specification distinguishes between
/// constraints (NOT NULL, UNIQUE, PRIMARY KEY, and CHECK), which can be named
/// and can appear in any order, and other options (DEFAULT, GENERATED), which
/// cannot be named and must appear in a fixed order. PostgreSQL, however,
/// allows preceding any option with `CONSTRAINT <name>`, even those that are
/// not really constraints, like NULL and DEFAULT. MSSQL is less permissive,
/// allowing DEFAULT, UNIQUE, PRIMARY KEY and CHECK to be named, but not NULL or
/// NOT NULL constraints (the last of which is in violation of the spec).
///
/// For maximum flexibility, we don't distinguish between constraint and
/// non-constraint options, lumping them all together under the umbrella of
/// "column options," and we allow any column option to be named.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnOptionDef<T: AstInfo> {
    pub name: Option<Ident>,
    pub option: ColumnOption<T>,
}

impl<T: AstInfo> AstDisplay for ColumnOptionDef<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&display_constraint_name(&self.name));
        f.write_node(&self.option);
    }
}
impl_display_t!(ColumnOptionDef);

/// `ColumnOption`s are modifiers that follow a column definition in a `CREATE
/// TABLE` statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColumnOption<T: AstInfo> {
    /// `NULL`
    Null,
    /// `NOT NULL`
    NotNull,
    /// `DEFAULT <restricted-expr>`
    Default(Expr<T>),
    /// `{ PRIMARY KEY | UNIQUE }`
    Unique {
        is_primary: bool,
    },
    /// A referential integrity constraint (`[FOREIGN KEY REFERENCES
    /// <foreign_table> (<referred_columns>)`).
    ForeignKey {
        foreign_table: UnresolvedObjectName,
        referred_columns: Vec<Ident>,
    },
    // `CHECK (<expr>)`
    Check(Expr<T>),
}

impl<T: AstInfo> AstDisplay for ColumnOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        use ColumnOption::*;
        match self {
            Null => f.write_str("NULL"),
            NotNull => f.write_str("NOT NULL"),
            Default(expr) => {
                f.write_str("DEFAULT ");
                f.write_node(expr);
            }
            Unique { is_primary } => {
                if *is_primary {
                    f.write_str("PRIMARY KEY");
                } else {
                    f.write_str("UNIQUE");
                }
            }
            ForeignKey {
                foreign_table,
                referred_columns,
            } => {
                f.write_str("REFERENCES ");
                f.write_node(foreign_table);
                f.write_str(" (");
                f.write_node(&display::comma_separated(referred_columns));
                f.write_str(")");
            }
            Check(expr) => {
                f.write_str("CHECK (");
                f.write_node(expr);
                f.write_str(")");
            }
        }
    }
}
impl_display_t!(ColumnOption);

fn display_constraint_name<'a>(name: &'a Option<Ident>) -> impl AstDisplay + 'a {
    struct ConstraintName<'a>(&'a Option<Ident>);
    impl<'a> AstDisplay for ConstraintName<'a> {
        fn fmt<W>(&self, f: &mut AstFormatter<W>)
        where
            W: fmt::Write,
        {
            if let Some(name) = self.0 {
                f.write_str("CONSTRAINT ");
                f.write_node(name);
                f.write_str(" ");
            }
        }
    }
    ConstraintName(name)
}
