# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

import pytest

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture, \
    assert_gpu_fallback_collect, assert_gpu_fallback_write_sql
from data_gen import copy_and_update
from iceberg import get_full_table_name, iceberg_unsupported_mark, iceberg_write_enabled_conf
from marks import allow_non_gpu, iceberg, ignore_order
from spark_session import with_cpu_session


_ICEBERG_V3_CONF = "spark.rapids.sql.format.iceberg.v3.enabled"


def _runtime_supports_iceberg_v3():
    version = os.environ.get("EXPECTED_ICEBERG_VERSION")
    if version is None:
        return False
    major_minor = tuple(int(part) for part in version.split(".")[:2])
    return major_minor >= (1, 9)


pytestmark = [
    iceberg_unsupported_mark,
    pytest.mark.skipif(
        not _runtime_supports_iceberg_v3(),
        reason="Iceberg format v3 requires Iceberg 1.9 or later"),
]


def _create_v3_table(spark, table_name):
    spark.sql(
        f"CREATE TABLE {table_name} (id BIGINT, data STRING) USING ICEBERG "
        "TBLPROPERTIES ('format-version' = '3', 'write.spark.fanout.enabled' = 'false')")


@iceberg
@ignore_order(local=True)
@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@pytest.mark.parametrize("v3_conf", [
    pytest.param({}, id="default-disabled"),
    pytest.param({_ICEBERG_V3_CONF: "false"}, id="explicitly-disabled"),
])
def test_iceberg_v3_read_fallback(spark_tmp_table_factory, v3_conf):
    table_name = get_full_table_name(spark_tmp_table_factory)

    def setup_table(spark):
        _create_v3_table(spark, table_name)
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    with_cpu_session(setup_table)
    assert_gpu_fallback_collect(
        lambda spark: spark.sql(f"SELECT * FROM {table_name}"),
        "BatchScanExec",
        conf=v3_conf)


@iceberg
@ignore_order(local=True)
def test_iceberg_v3_read_enabled(spark_tmp_table_factory):
    table_name = get_full_table_name(spark_tmp_table_factory)

    def setup_table(spark):
        _create_v3_table(spark, table_name)
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    with_cpu_session(setup_table)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(f"SELECT * FROM {table_name}"),
        exist_classes="GpuBatchScanExec",
        conf={_ICEBERG_V3_CONF: "true"})


@iceberg
@ignore_order(local=True)
@allow_non_gpu("AppendDataExec")
@pytest.mark.parametrize("v3_conf", [
    pytest.param({}, id="default-disabled"),
    pytest.param({_ICEBERG_V3_CONF: "false"}, id="explicitly-disabled"),
])
def test_iceberg_v3_append_fallback(spark_tmp_table_factory, v3_conf):
    base_table_name = get_full_table_name(spark_tmp_table_factory)

    with_cpu_session(lambda spark: _create_v3_table(spark, f"{base_table_name}_cpu"))
    with_cpu_session(lambda spark: _create_v3_table(spark, f"{base_table_name}_gpu"))

    def append_data(spark, table_name):
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    assert_gpu_fallback_write_sql(
        append_data,
        lambda spark, table_name: spark.sql(f"SELECT * FROM {table_name}"),
        base_table_name,
        ["AppendDataExec"],
        conf=copy_and_update(iceberg_write_enabled_conf, v3_conf))


@iceberg
@ignore_order(local=True)
@allow_non_gpu("AtomicCreateTableAsSelectExec")
def test_iceberg_v3_ctas_fallback(spark_tmp_table_factory):
    base_table_name = get_full_table_name(spark_tmp_table_factory)

    def create_table(spark, table_name):
        spark.sql(
            f"CREATE TABLE {table_name} USING ICEBERG "
            "TBLPROPERTIES ('format-version' = '3', 'write.spark.fanout.enabled' = 'false') "
            "AS SELECT * FROM VALUES (1L, 'a'), (2L, 'b'), (3L, 'c') AS source(id, data)")

    assert_gpu_fallback_write_sql(
        create_table,
        lambda spark, table_name: spark.sql(f"SELECT * FROM {table_name}"),
        base_table_name,
        ["AtomicCreateTableAsSelectExec"],
        conf=iceberg_write_enabled_conf)
