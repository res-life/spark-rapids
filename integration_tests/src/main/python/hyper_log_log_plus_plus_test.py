# Copyright (c) 2021-2024, NVIDIA CORPORATION.
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

import pytest

from asserts import assert_gpu_and_cpu_are_equal_sql, run_with_cpu
from data_gen import *

# Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from data_gen import *
from marks import allow_non_gpu, ignore_order

_xxhash_gens = [
    null_gen,
    boolean_gen,
    byte_gen,
    short_gen,
    int_gen,
    long_gen,
    date_gen,
    timestamp_gen,
    decimal_gen_32bit,
    decimal_gen_64bit,
    decimal_gen_128bit,
    float_gen,
    double_gen
]

_struct_of_xxhash_gens = StructGen([(f"c{i}", g) for i, g in enumerate(_xxhash_gens)])

_xxhash_gens = (_xxhash_gens + [_struct_of_xxhash_gens] + single_level_array_gens
                + nested_array_gens_sample + [
                    all_basic_struct_gen,
                    struct_array_gen,
                    _struct_of_xxhash_gens
                ] + map_gens_sample)

@pytest.mark.parametrize('data_gen', _xxhash_gens, ids=idfn)
def test_hllpp_groupby(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, [("c1", int_gen), ("c2", data_gen)]),
        "tab",
        "select c1, APPROX_COUNT_DISTINCT(c2) from tab group by c1")


@pytest.mark.parametrize('data_gen', _xxhash_gens, ids=idfn)
def test_hllpp_reduction(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : unary_op_df(spark, data_gen, length=10),
        "tab",
        "select APPROX_COUNT_DISTINCT(a) from tab",
        {"spark.sql.legacy.allowHashOnMapType": True}
    )

def test_hllpp_double():
    run_with_cpu(lambda spark : unary_op_df(spark, DoubleGen(nullable=False), length = 2000), "COLLECT")

    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : unary_op_df(spark, DoubleGen(nullable=False), length = 2000),
        "tab",
        "select APPROX_COUNT_DISTINCT(a) from tab",
        {"spark.sql.legacy.allowHashOnMapType": True},
        debug = True
    )

@pytest.mark.parametrize('data_gen', _xxhash_gens, ids=idfn)
def test_1xxhash64(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : unary_op_df(spark, data_gen),
        "tab",
        "select xxhash64(a) from tab",
      {"spark.sql.legacy.allowHashOnMapType": True})

