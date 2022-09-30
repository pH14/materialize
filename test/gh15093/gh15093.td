# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


$ set key-schema={
    "type": "string"
  }

$ set value-schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"}
        ]
    }

$ kafka-create-topic topic=bounded-memory

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "A1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE SOURCE bounded_memory_source
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-bounded-memory-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

> CREATE MATERIALIZED VIEW bounded_memory_view AS SELECT COUNT(*) FROM bounded_memory_source;

> SELECT * FROM bounded_memory_view;
1000000

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "B1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "C1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "D1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "E1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "F1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "G1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "H1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "I1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "J1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "B1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "C1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "D1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "E1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "F1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "G1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "H1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "I1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "J1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "B1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "C1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "D1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "E1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "F1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "G1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "H1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "I1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "J1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "B1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "C1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "D1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "E1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "F1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "G1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "H1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "I1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "J1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "B1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "C1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "D1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "E1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "F1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "G1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "H1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "I1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "J1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "B1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "C1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "D1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "E1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "F1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "G1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "H1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "I1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "J1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "B1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "C1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "D1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "E1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "F1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "G1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "H1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "I1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "J1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "B1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "C1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "D1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "E1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "F1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "G1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "H1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "I1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "J1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "B1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "C1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "D1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "E1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "F1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "G1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "H1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "I1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "J1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "B1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "C1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "D1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "E1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "F1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "G1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "H1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "I1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}" {"f1": "J1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"A" {"f1": "EOF"}

> SELECT * FROM bounded_memory_view;
1000001
