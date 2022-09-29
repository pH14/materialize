# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition
from materialize.mzcompose.services import Materialized, Redpanda, Testdrive, Localstack

SERVICES = [
    Localstack(),
    Materialized(
# memory="2Gb",
# extra_ports=["2104:2104"], allow_host_ports=True
    persist_blob_url = 's3://minio:minio123@persist/persist?endpoint=http://localstack:4566/&region=minio',
),
    Testdrive(no_reset=True, seed=1, default_timeout="3600s"),
    Redpanda(),
]


def workflow_default(c: Composition) -> None:
    """Use a Mz instance with 2Gb of RAM to ingest a >10G Postgres dataset"""

    c.start_and_wait_for_tcp(services=["localstack", "redpanda"])
    c.exec("localstack", "awslocal", "s3api", "create-bucket", "--bucket", "persist")


    c.start_and_wait_for_tcp(services=["materialized"])
    c.wait_for_materialized()
    c.run("testdrive", "bounded-memory-before-restart.td")

    # Restart Mz to confirm that re-hydration is also bounded memory
    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized()

    c.run("testdrive", "bounded-memory-after-restart.td")
