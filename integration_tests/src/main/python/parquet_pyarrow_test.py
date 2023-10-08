# Copyright (c) 2023, NVIDIA CORPORATION.
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

from pyarrow_utils import assert_gpu_and_pyarrow_are_compatible
from data_gen import *
from parquet_test import reader_opt_confs

# types for test_parquet_read_round_trip_for_pyarrow
sub_gens = all_basic_gens_no_null + [decimal_gen_64bit, decimal_gen_128bit]

struct_gen = StructGen([('child_' + str(i), sub_gens[i]) for i in range(len(sub_gens))])
array_gens = [ArrayGen(sub_gen) for sub_gen in sub_gens]
parquet_gens_list = [
    [binary_gen],
    sub_gens,
    [struct_gen],
    single_level_array_gens_no_null,
    map_gens_sample,
]


#
# test read/write by pyarrow/GPU
#
@pytest.mark.parametrize('parquet_gens', parquet_gens_list, ids=idfn)
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_read_round_trip_for_pyarrow(
        spark_tmp_path,
        parquet_gens,
        reader_confs,
        v1_enabled_list):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        # set the int96 rebase mode values because its LEGACY in databricks which will preclude this op from running on GPU
        'spark.sql.legacy.parquet.int96RebaseModeInRead': 'CORRECTED',
        'spark.sql.legacy.parquet.int96RebaseModeInWrite': 'CORRECTED',
        'spark.sql.parquet.int96RebaseModeInRead': 'CORRECTED',
        'spark.sql.parquet.int96RebaseModeInWrite': 'CORRECTED',

        'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'CORRECTED',
        'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
        'spark.sql.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
        'spark.sql.parquet.datetimeRebaseModeInRead': 'CORRECTED'})

    # once https://github.com/NVIDIA/spark-rapids/issues/1126 is in we can remove spark.sql.legacy.parquet.datetimeRebaseModeInRead config which is a workaround
    # for nested timestamp/date support
    assert_gpu_and_pyarrow_are_compatible(spark_tmp_path, gen_list, conf=all_confs)
