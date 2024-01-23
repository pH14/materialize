# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Any

from psycopg import Cursor


class OperationData:
    def __init__(self, cursor: Cursor):
        self._data: dict[str, Any] = dict()
        self._data["cursor"] = cursor

    def cursor(self) -> Cursor:
        return self._data["cursor"]

    def push(self, key: str, value: Any) -> None:
        self._data[key] = value

    def get(self, key: str) -> Any:
        if key not in self._data.keys():
            raise RuntimeError(f"Key does not exist: {key}")

        return self._data[key]

    def validate_requirements(
        self, expected_keys: set[str], required_by: type[Any], requirement: str
    ) -> None:
        for key in expected_keys:
            if key not in self._data.keys():
                raise RuntimeError(
                    f"{required_by.__name__} {requirement} '{key}' but got only: {self._data.keys()}"
                )
