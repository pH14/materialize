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

> SELECT * FROM bounded_memory_view;
1000001

$ kafka-ingest format=avro key-format=avro topic=bounded-memory schema=${value-schema} key-schema=${key-schema} repeat=1000000
"${kafka-ingest.iteration}"

> SELECT * FROM bounded_memory_view;
1
