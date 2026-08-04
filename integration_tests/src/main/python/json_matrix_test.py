# Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

import pyspark.sql.functions as f
import pytest

from asserts import *
from data_gen import *
from marks import approximate_float, allow_non_gpu, ignore_order, datagen_overrides
from spark_session import *

def read_json_df(data_path, schema, spark_tmp_table_factory_ignored, options = {}):
    def read_impl(spark):
        reader = spark.read
        if not schema is None:
            reader = reader.schema(schema)
        for key, value in options.items():
            reader = reader.option(key, value)
        return reader.json(data_path)
    return read_impl

def read_json_sql(data_path, schema, spark_tmp_table_factory, options = {}):
    opts = options
    if not schema is None:
        opts = copy_and_update(options, {'schema': schema})
    def read_impl(spark):
        tmp_name = spark_tmp_table_factory.get()
        return spark.catalog.createTable(tmp_name, source='json', path=data_path, **opts)
    return read_impl

def read_json_as_text(spark, data_path, column_name):
    return spark.read.text(data_path).withColumnRenamed("value", column_name)

TEXT_INPUT_EXEC='FileSourceScanExec'

_enable_all_types_json_scan_conf = {
    'spark.rapids.sql.format.json.enabled': 'true',
    'spark.rapids.sql.format.json.read.enabled': 'true',
    'spark.rapids.sql.json.read.float.enabled': 'true',
    'spark.rapids.sql.json.read.double.enabled': 'true',
    'spark.rapids.sql.json.read.decimal.enabled': 'true'
}

_enable_json_to_structs_conf = {
    'spark.rapids.sql.expression.JsonToStructs': 'true',
    'spark.rapids.sql.json.read.float.enabled': 'true',
    'spark.rapids.sql.json.read.double.enabled': 'true',
    'spark.rapids.sql.json.read.decimal.enabled': 'true',
    'spark.rapids.sql.json.read.decimal.enabled': 'true'
}

_enable_json_scan_and_to_structs_conf = copy_and_update(
    _enable_all_types_json_scan_conf, _enable_json_to_structs_conf)

WITH_COMMENTS_FILE = "withComments.json"
WITH_COMMENTS_SCHEMA = StructType([StructField("str", StringType())])
WITH_COMMENTS_MAP_SCHEMA = MapType(StringType(), StringType())

@allow_non_gpu('FileSourceScanExec')
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_comments_on(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_fallback_collect(
        read_func(std_input_path + '/' + WITH_COMMENTS_FILE,
        WITH_COMMENTS_SCHEMA,
        spark_tmp_table_factory,
        {"allowComments": "true"}),
        'FileSourceScanExec',
        conf=_enable_all_types_json_scan_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, 'JsonToStructs')
def test_from_json_allow_comments_on(std_input_path):
    schema = WITH_COMMENTS_SCHEMA
    assert_gpu_fallback_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_COMMENTS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowComments': "true"})),
        'JsonToStructs',
        conf =_enable_json_to_structs_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, 'JsonToStructs')
def test_from_json_allow_comments_on_map(std_input_path):
    schema = WITH_COMMENTS_MAP_SCHEMA
    assert_gpu_fallback_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_COMMENTS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowComments': "true"})),
        'JsonToStructs',
        conf =_enable_json_to_structs_conf)

# Off is the default so it really needs to work
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_comments_off(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_COMMENTS_FILE,
        WITH_COMMENTS_SCHEMA,
        spark_tmp_table_factory,
        {"allowComments": "false"}),
        conf=_enable_all_types_json_scan_conf)

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_comments_off(std_input_path):
    schema = WITH_COMMENTS_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_COMMENTS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowComments': "false"})),
        conf =_enable_json_to_structs_conf)

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_comments_off_map(std_input_path):
    schema = WITH_COMMENTS_MAP_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_COMMENTS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowComments': "false"})),
        conf =_enable_json_to_structs_conf)

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_get_json_object_allow_comments_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_COMMENTS_FILE, "json").selectExpr('''get_json_object(json, "$.str")'''))

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_json_tuple_allow_comments_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_COMMENTS_FILE, "json").selectExpr('''json_tuple(json, "str")'''))

WITH_SQ_FILE = "withSingleQuotes.json"
WITH_SQ_SCHEMA = StructType([StructField("str", StringType())])
WITH_SQ_MAP_SCHEMA = MapType(StringType(), StringType())

@allow_non_gpu('FileSourceScanExec')
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_single_quotes_off(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_SQ_FILE,
        WITH_SQ_SCHEMA,
        spark_tmp_table_factory,
        {"allowSingleQuotes": "false"}),
        conf=_enable_all_types_json_scan_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, 'JsonToStructs')
def test_from_json_allow_single_quotes_off(std_input_path):
    schema = WITH_SQ_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_SQ_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowSingleQuotes': "false"})),
        conf =_enable_json_to_structs_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, 'JsonToStructs')
def test_from_json_allow_single_quotes_off_map(std_input_path):
    schema = WITH_SQ_MAP_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_SQ_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowSingleQuotes': "false"})),
        conf =_enable_json_to_structs_conf)

# On is the default so it really needs to work
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_single_quotes_on(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_SQ_FILE,
        WITH_SQ_SCHEMA,
        spark_tmp_table_factory,
        {"allowSingleQuotes": "true"}),
        conf=_enable_all_types_json_scan_conf)

# On is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_single_quotes_on(std_input_path):
    schema = WITH_SQ_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_SQ_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowSingleQuotes': "true"})),
        conf =_enable_json_to_structs_conf)

# On is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_single_quotes_on_map(std_input_path):
    schema = WITH_SQ_MAP_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_SQ_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowSingleQuotes': "true"})),
        conf =_enable_json_to_structs_conf)

# On is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_get_json_object_allow_single_quotes_on(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_SQ_FILE, "json").selectExpr('''get_json_object(json, "$.str")'''))

# On is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_json_tuple_allow_single_quotes_on(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_SQ_FILE, "json").selectExpr('''json_tuple(json, "str")'''))

WITH_UNQUOTE_FIELD_NAMES_FILE = "withUnquotedFieldNames.json"
WITH_UNQUOTE_FIELD_NAMES_SCHEMA = StructType([StructField("str", StringType())])
WITH_UNQUOTE_FIELD_NAMES_MAP_SCHEMA = MapType(StringType(), StringType())

@allow_non_gpu('FileSourceScanExec')
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_unquoted_field_names_on(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_fallback_collect(
        read_func(std_input_path + '/' + WITH_UNQUOTE_FIELD_NAMES_FILE,
        WITH_UNQUOTE_FIELD_NAMES_SCHEMA,
        spark_tmp_table_factory,
        {"allowUnquotedFieldNames": "true"}),
        'FileSourceScanExec',
        conf=_enable_all_types_json_scan_conf)

@allow_non_gpu('JsonToStructs', TEXT_INPUT_EXEC)
def test_from_json_allow_unquoted_field_names_on(std_input_path):
    schema = WITH_UNQUOTE_FIELD_NAMES_SCHEMA
    assert_gpu_fallback_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTE_FIELD_NAMES_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowUnquotedFieldNames': "true"})),
        'JsonToStructs',
        conf =_enable_json_to_structs_conf)

@allow_non_gpu('JsonToStructs', TEXT_INPUT_EXEC)
def test_from_json_allow_unquoted_field_names_on_map(std_input_path):
    schema = WITH_UNQUOTE_FIELD_NAMES_MAP_SCHEMA
    assert_gpu_fallback_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTE_FIELD_NAMES_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowUnquotedFieldNames': "true"})),
        'JsonToStructs',
        conf =_enable_json_to_structs_conf)

# Off is the default so it really needs to work
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_unquoted_field_names_off(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_UNQUOTE_FIELD_NAMES_FILE,
        WITH_UNQUOTE_FIELD_NAMES_SCHEMA,
        spark_tmp_table_factory,
        {"allowUnquotedFieldNames": "false"}),
        conf=_enable_all_types_json_scan_conf)

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_unquoted_field_names_off(std_input_path):
    schema = WITH_UNQUOTE_FIELD_NAMES_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTE_FIELD_NAMES_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowUnquotedFieldNames': "false"})),
        conf =_enable_json_to_structs_conf)

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_unquoted_field_names_off_map(std_input_path):
    schema = WITH_UNQUOTE_FIELD_NAMES_MAP_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTE_FIELD_NAMES_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowUnquotedFieldNames': "false"})),
        conf =_enable_json_to_structs_conf)

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_get_json_object_allow_unquoted_field_names_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTE_FIELD_NAMES_FILE, "json").selectExpr('''get_json_object(json, "$.str")'''))

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_json_tuple_allow_unquoted_field_names_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTE_FIELD_NAMES_FILE, "json").selectExpr('''json_tuple(json, "str")'''))

WITH_NUMERIC_LEAD_ZEROS_FILE = "withNumericLeadingZeros.json"
WITH_NUMERIC_LEAD_ZEROS_SCHEMA = StructType([StructField("byte", ByteType()),
    StructField("int", IntegerType()),
    StructField("float", FloatType()),
    StructField("decimal", DecimalType(10, 3))])
WITH_NUMERIC_LEAD_ZEROS_MAP_SCHEMA = MapType(StringType(), StringType())

@approximate_float()
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_numeric_leading_zeros_on(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_NUMERIC_LEAD_ZEROS_FILE,
        WITH_NUMERIC_LEAD_ZEROS_SCHEMA,
        spark_tmp_table_factory,
        {"allowNumericLeadingZeros": "true"}),
        conf=_enable_all_types_json_scan_conf)

@approximate_float()
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_numeric_leading_zeros_on(std_input_path):
    schema = WITH_NUMERIC_LEAD_ZEROS_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NUMERIC_LEAD_ZEROS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowNumericLeadingZeros": "true"})),
        conf =_enable_json_to_structs_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_numeric_leading_zeros_on_map(std_input_path):
    schema = WITH_NUMERIC_LEAD_ZEROS_MAP_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NUMERIC_LEAD_ZEROS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowNumericLeadingZeros": "true"})),
        conf =_enable_json_to_structs_conf)

# Off is the default so it really needs to work
@approximate_float()
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_numeric_leading_zeros_off(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_NUMERIC_LEAD_ZEROS_FILE,
        WITH_NUMERIC_LEAD_ZEROS_SCHEMA,
        spark_tmp_table_factory,
        {"allowNumericLeadingZeros": "false"}),
        conf=_enable_all_types_json_scan_conf)

# Off is the default so it really needs to work
@approximate_float()
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_numeric_leading_zeros_off(std_input_path):
    schema = WITH_NUMERIC_LEAD_ZEROS_SCHEMA

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NUMERIC_LEAD_ZEROS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowNumericLeadingZeros": "false"})),
        conf =_enable_json_to_structs_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_numeric_leading_zeros_off_map(std_input_path):
    schema = WITH_NUMERIC_LEAD_ZEROS_MAP_SCHEMA

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NUMERIC_LEAD_ZEROS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowNumericLeadingZeros": "false"})),
        conf =_enable_json_to_structs_conf)

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_get_json_object_allow_numeric_leading_zeros_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NUMERIC_LEAD_ZEROS_FILE, "json").selectExpr('''get_json_object(json, "$.byte")''',
            '''get_json_object(json, "$.int")''', '''get_json_object(json, "$.float")''','''get_json_object(json, "$.decimal")'''))

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_json_tuple_allow_numeric_leading_zeros_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NUMERIC_LEAD_ZEROS_FILE, "json").selectExpr('''json_tuple(json, "byte", "int", "float", "decimal")'''))

WITH_NONNUMERIC_NUMBERS_FILE = "withNonnumericNumbers.json"
WITH_NONNUMERIC_NUMBERS_SCHEMA = StructType([
    StructField("float", FloatType()),
    StructField("double", DoubleType())])
WITH_NONNUMERIC_NUMBERS_MAP_SCHEMA = MapType(StringType(), StringType())

@approximate_float()
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_nonnumeric_numbers_off(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_NONNUMERIC_NUMBERS_FILE,
        WITH_NONNUMERIC_NUMBERS_SCHEMA,
        spark_tmp_table_factory,
        {"allowNonNumericNumbers": "false"}),
        conf=_enable_all_types_json_scan_conf)

@approximate_float()
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_nonnumeric_numbers_off(std_input_path):
    schema = WITH_NONNUMERIC_NUMBERS_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NONNUMERIC_NUMBERS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowNonNumericNumbers": "false"})),
        conf =_enable_json_to_structs_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_nonnumeric_numbers_off_map(std_input_path):
    schema = WITH_NONNUMERIC_NUMBERS_MAP_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NONNUMERIC_NUMBERS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowNonNumericNumbers": "false"})),
        conf =_enable_json_to_structs_conf)

# On is the default for scan so it really needs to work
@approximate_float()
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_nonnumeric_numbers_on(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_NONNUMERIC_NUMBERS_FILE,
        WITH_NONNUMERIC_NUMBERS_SCHEMA,
        spark_tmp_table_factory,
        {"allowNonNumericNumbers": "true"}),
        conf=_enable_all_types_json_scan_conf)

# On is the default for from_json so it really needs to work
@approximate_float()
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_nonnumeric_numbers_on(std_input_path):
    schema = WITH_NONNUMERIC_NUMBERS_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NONNUMERIC_NUMBERS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowNonNumericNumbers": "true"})),
        conf =_enable_json_to_structs_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_nonnumeric_numbers_on_map(std_input_path):
    schema = WITH_NONNUMERIC_NUMBERS_MAP_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NONNUMERIC_NUMBERS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowNonNumericNumbers": "true"})),
        conf =_enable_json_to_structs_conf)

# Off is the default for get_json_object so we want this to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_get_json_object_allow_nonnumeric_numbers_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NONNUMERIC_NUMBERS_FILE, "json").selectExpr('''get_json_object(json, "$.float")''',
            '''get_json_object(json, "$.double")'''))

# Off is the default for json_tuple, so we want this to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_json_tuple_allow_nonnumeric_numbers_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NONNUMERIC_NUMBERS_FILE, "json").selectExpr('''json_tuple(json, "float", "double")'''))

WITH_BS_ESC_FILE = "withBackslashEscapingAnyCharacter.json"
WITH_BS_ESC_SCHEMA = StructType([
    StructField("str", StringType())])
WITH_BS_ESC_MAP_SCHEMA = MapType(StringType(), StringType())

# Off is the default for scan so it really needs to work
@pytest.mark.parametrize('read_func', [read_json_df]) # we have done so many tests already that we don't need both read func. They are the same
def test_scan_json_allow_backslash_escape_any_off(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_BS_ESC_FILE,
        WITH_BS_ESC_SCHEMA,
        spark_tmp_table_factory,
        {"allowBackslashEscapingAnyCharacter": "false"}),
        conf=_enable_all_types_json_scan_conf)

# Off is the default for from_json so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_backslash_escape_any_off(std_input_path):
    schema = WITH_BS_ESC_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_BS_ESC_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowBackslashEscapingAnyCharacter": "false"})),
        conf =_enable_json_to_structs_conf)

# Off is the default for from_json so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_backslash_escape_any_off_map(std_input_path):
    schema = WITH_BS_ESC_MAP_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_BS_ESC_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowBackslashEscapingAnyCharacter": "false"})),
        conf =_enable_json_to_structs_conf)

@allow_non_gpu('FileSourceScanExec')
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_backslash_escape_any_on(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_fallback_collect(
        read_func(std_input_path + '/' + WITH_BS_ESC_FILE,
        WITH_BS_ESC_SCHEMA,
        spark_tmp_table_factory,
        {"allowBackslashEscapingAnyCharacter": "true"}),
        'FileSourceScanExec',
        conf=_enable_all_types_json_scan_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, 'JsonToStructs')
def test_from_json_allow_backslash_escape_any_on(std_input_path):
    schema = WITH_BS_ESC_SCHEMA
    assert_gpu_fallback_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_BS_ESC_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowBackslashEscapingAnyCharacter": "true"})),
        'JsonToStructs',
        conf =_enable_json_to_structs_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, 'JsonToStructs')
def test_from_json_allow_backslash_escape_any_on_map(std_input_path):
    schema = WITH_BS_ESC_MAP_SCHEMA
    assert_gpu_fallback_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_BS_ESC_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowBackslashEscapingAnyCharacter": "true"})),
        'JsonToStructs',
        conf =_enable_json_to_structs_conf)

# Off is the default for get_json_object so we want this to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_get_json_object_allow_backslash_escape_any_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_BS_ESC_FILE, "json").selectExpr('''get_json_object(json, "$.str")'''))

# Off is the default for json_tuple, so we want this to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_json_tuple_allow_backslash_escape_any_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_BS_ESC_FILE, "json").selectExpr('''json_tuple(json, "str")'''))

WITH_UNQUOTED_CONTROL_FILE = "withUnquotedControlChars.json"
WITH_UNQUOTED_CONTROL_SCHEMA = StructType([
    StructField("str", StringType())])
WITH_UNQUOTED_CONTROL_MAP_SCHEMA = MapType(StringType(), StringType())

# Off is the default for scan so it really needs to work
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_unquoted_control_chars_off(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_UNQUOTED_CONTROL_FILE,
        WITH_UNQUOTED_CONTROL_SCHEMA,
        spark_tmp_table_factory,
        {"allowUnquotedControlChars": "false"}),
        conf=_enable_all_types_json_scan_conf)

# Off is the default for from_json so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_unquoted_control_chars_off(std_input_path):
    schema = WITH_UNQUOTED_CONTROL_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTED_CONTROL_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowUnquotedControlChars": "false"})),
        conf =_enable_json_to_structs_conf)

# Off is the default for from_json so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_unquoted_control_chars_off_map(std_input_path):
    schema = WITH_UNQUOTED_CONTROL_MAP_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTED_CONTROL_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowUnquotedControlChars": "false"})),
        conf =_enable_json_to_structs_conf)

@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_unquoted_control_chars_on(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_UNQUOTED_CONTROL_FILE,
        WITH_UNQUOTED_CONTROL_SCHEMA,
        spark_tmp_table_factory,
        {"allowUnquotedControlChars": "true"}),
        conf=_enable_all_types_json_scan_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_unquoted_control_chars_on(std_input_path):
    schema = WITH_UNQUOTED_CONTROL_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTED_CONTROL_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowUnquotedControlChars": "true"})),
        conf =_enable_json_to_structs_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_unquoted_control_chars_on_map(std_input_path):
    schema = WITH_UNQUOTED_CONTROL_MAP_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTED_CONTROL_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowUnquotedControlChars": "true"})),
        conf =_enable_json_to_structs_conf)

# On is the default for get_json_object so we want this to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_get_json_object_allow_unquoted_control_chars_on(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTED_CONTROL_FILE, "json").selectExpr('''get_json_object(json, "$.str")'''))

# On is the default for json_tuple, so we want this to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_json_tuple_allow_unquoted_control_chars_on(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTED_CONTROL_FILE, "json").selectExpr('''json_tuple(json, "str")'''))


WITH_DEC_LOCALE_FILE = "decimal_locale_formatted_strings.json"
WITH_DEC_LOCALE_NON_ARIBIC_FILE = "decimal_locale_formatted_strings_non_aribic.json"
WITH_DEC_LOCALE_SCHEMA = StructType([
    StructField("data", DecimalType(10, 5))])
WITH_DEC_LOCALE_MAP_SCHEMA = MapType(StringType(), StringType())
NON_US_DEC_LOCALES=["it-CH","ko-KR","h-TH-x-lvariant-TH","ru-RU","de-DE","iw-IL","hi-IN","ar-QA","zh-CN","ko-KR"]

# US is the default locale so we kind of what it to work
@pytest.mark.parametrize('read_func', [read_json_df]) # We don't need both they are always the same
def test_scan_json_dec_locale_US(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_DEC_LOCALE_FILE,
        WITH_DEC_LOCALE_SCHEMA,
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

# We don't support the other locales yet, so we fall back to the CPU
@allow_non_gpu('FileSourceScanExec')
@pytest.mark.parametrize('read_func', [read_json_df]) # We don't need both they are always the same
@pytest.mark.parametrize('locale', NON_US_DEC_LOCALES)
def test_scan_json_dec_locale(std_input_path, read_func, spark_tmp_table_factory, locale):
    assert_gpu_fallback_collect(
        read_func(std_input_path + '/' + WITH_DEC_LOCALE_FILE,
        WITH_DEC_LOCALE_SCHEMA,
        spark_tmp_table_factory,
        {"locale": locale}),
        'FileSourceScanExec',
        conf=_enable_all_types_json_scan_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_dec_locale_US(std_input_path):
    schema = WITH_DEC_LOCALE_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_DEC_LOCALE_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, 'JsonToStructs', *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
@pytest.mark.parametrize('locale', NON_US_DEC_LOCALES)
def test_from_json_dec_locale(std_input_path, locale):
    schema = WITH_DEC_LOCALE_SCHEMA
    assert_gpu_fallback_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_DEC_LOCALE_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"locale": locale})),
        'JsonToStructs',
        conf =_enable_json_to_structs_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_dec_locale_US_map(std_input_path):
    schema = WITH_DEC_LOCALE_MAP_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_DEC_LOCALE_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

# This will not fall back because we only support map<string, string>
# and locals impact decimal parsing, not strings.
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
@pytest.mark.parametrize('locale', NON_US_DEC_LOCALES)
def test_from_json_dec_locale_map(std_input_path, locale):
    schema = WITH_DEC_LOCALE_MAP_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_DEC_LOCALE_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"locale": locale})),
        conf =_enable_json_to_structs_conf)

#There is no way to set a locale for these, and it really should not matter
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_get_json_object_dec_locale(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_DEC_LOCALE_FILE, "json").selectExpr('''get_json_object(json, "$.data")'''))

#There is no way to set a locale for these, and it really should not matter
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_json_tuple_dec_locale(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_DEC_LOCALE_FILE, "json").selectExpr('''json_tuple(json, "data")'''))


####################################################################
# Spark supports non-aribic numbers, but they should be really rare
####################################################################

# US is the default locale so we kind of what it to work
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/10532')
@pytest.mark.parametrize('read_func', [read_json_df]) # We don't need both they are always the same
def test_scan_json_dec_locale_US_non_aribic(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_DEC_LOCALE_NON_ARIBIC_FILE,
        WITH_DEC_LOCALE_SCHEMA,
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

# We don't support the other locales yet, so we fall back to the CPU
@allow_non_gpu('FileSourceScanExec')
@pytest.mark.parametrize('read_func', [read_json_df]) # We don't need both they are always the same
@pytest.mark.parametrize('locale', NON_US_DEC_LOCALES)
def test_scan_json_dec_locale_non_aribic(std_input_path, read_func, spark_tmp_table_factory, locale):
    assert_gpu_fallback_collect(
        read_func(std_input_path + '/' + WITH_DEC_LOCALE_NON_ARIBIC_FILE,
        WITH_DEC_LOCALE_SCHEMA,
        spark_tmp_table_factory,
        {"locale": locale}),
        'FileSourceScanExec',
        conf=_enable_all_types_json_scan_conf)

@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/10532')
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_dec_locale_US_non_aribic(std_input_path):
    schema = WITH_DEC_LOCALE_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_DEC_LOCALE_NON_ARIBIC_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, 'JsonToStructs', *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
@pytest.mark.parametrize('locale', NON_US_DEC_LOCALES)
def test_from_json_dec_locale_non_aribic(std_input_path, locale):
    schema = WITH_DEC_LOCALE_SCHEMA
    assert_gpu_fallback_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_DEC_LOCALE_NON_ARIBIC_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"locale": locale})),
        'JsonToStructs',
        conf =_enable_json_to_structs_conf)

# This will not fail because we only support map<string, string>
# and decimal is needed to trigger the translation issue
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_dec_locale_US_non_aribic_map(std_input_path):
    schema = WITH_DEC_LOCALE_MAP_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_DEC_LOCALE_NON_ARIBIC_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

# This will not fall back because we only support map<string, string>
# and locals impact decimal parsing, not strings.
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
@pytest.mark.parametrize('locale', NON_US_DEC_LOCALES)
def test_from_json_dec_locale_non_aribic_map(std_input_path, locale):
    schema = WITH_DEC_LOCALE_MAP_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_DEC_LOCALE_NON_ARIBIC_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"locale": locale})),
        conf =_enable_json_to_structs_conf)

#There is no way to set a locale for these, and it really should not matter
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_get_json_object_dec_locale_non_aribic(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_DEC_LOCALE_NON_ARIBIC_FILE, "json").selectExpr('''get_json_object(json, "$.data")'''))

#There is no way to set a locale for these, and it really should not matter
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_json_tuple_dec_locale_non_aribic(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_DEC_LOCALE_NON_ARIBIC_FILE, "json").selectExpr('''json_tuple(json, "data")'''))

# These are common files used by most of the tests. A few files are for specific types, but these are very targeted tests
COMMON_TEST_FILES=[
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    "int_array_formatted.json",
    "int_struct_formatted.json",
    "int_struct_formatted_problematic_rows.json",
    "int_mixed_array_struct_formatted.json",
    "bad_whitespace.json",
    "escaped_strings.json",
    "nested_escaped_strings.json",
    pytest.param("repeated_columns.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11361')),
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json"]

COMMON_SCAN_TEST_FILES = COMMON_TEST_FILES + [
    "scan_emtpy_lines.json"]

COMMON_PRIMITIVE_SCHEMAS = [
    StructType([StructField("data", data_type)])
    for data_type in [ByteType(), ShortType(), IntegerType(), LongType(), BooleanType()]]


def _scan_and_from_json(spark, data_path, schema):
    scan_df = read_json_df(data_path, schema, None)(spark).select(
        f.lit("scan").alias("source"), f.col("data"))
    from_json_df = read_json_as_text(spark, data_path, "json") \
        .select(f.from_json(f.col("json"), schema).alias("parsed")) \
        .select(f.lit("from_json").alias("source"), f.col("parsed.data").alias("data"))
    return scan_df.unionByName(from_json_df)


@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    "int_array_formatted.json",
    "int_struct_formatted.json",
    "int_mixed_array_struct_formatted.json",
    "bad_whitespace.json",
    "escaped_strings.json",
    "nested_escaped_strings.json",
    "repeated_columns.json", # This works for maps, but not others.
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json"])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_map_string_string(std_input_path, input_file):
    schema = MapType(StringType(), StringType())
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

@pytest.mark.parametrize('schema', COMMON_PRIMITIVE_SCHEMAS, ids=idfn)
@pytest.mark.parametrize('input_file', COMMON_TEST_FILES)
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow)
def test_scan_and_from_json_primitives(std_input_path, input_file, schema):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: _scan_and_from_json(
            spark, std_input_path + '/' + input_file, schema),
        conf=_enable_json_scan_and_to_structs_conf)


@pytest.mark.parametrize('schema', COMMON_PRIMITIVE_SCHEMAS, ids=idfn)
def test_scan_json_primitives_empty_lines(std_input_path, schema):
    assert_gpu_and_cpu_are_equal_collect(
        read_json_df(
            std_input_path + '/scan_emtpy_lines.json', schema, None),
        conf=_enable_all_types_json_scan_conf)

DECIMAL_TEST_FILES = [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    "invalid_ridealong_columns.json",
    "int_array_formatted.json",
    "int_struct_formatted.json",
    "int_struct_formatted_problematic_rows.json",
    "int_mixed_array_struct_formatted.json",
    "bad_whitespace.json",
    "escaped_strings.json",
    "nested_escaped_strings.json",
    pytest.param("repeated_columns.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11361')),
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json"]

DECIMAL_SCHEMAS = [
    StructType([StructField("data", data_type)])
    for data_type in [DecimalType(38, 0), DecimalType(38, 10), DecimalType(10, 2)]]


@pytest.mark.parametrize('schema', DECIMAL_SCHEMAS, ids=idfn)
@pytest.mark.parametrize('input_file', DECIMAL_TEST_FILES)
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow)
def test_scan_and_from_json_decimals(std_input_path, input_file, schema):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: _scan_and_from_json(
            spark, std_input_path + '/' + input_file, schema),
        conf=_enable_json_scan_and_to_structs_conf)

@pytest.mark.parametrize('schema', DECIMAL_SCHEMAS, ids=idfn)
def test_scan_json_decimals_empty_lines(std_input_path, schema):
    assert_gpu_and_cpu_are_equal_collect(
        read_json_df(
            std_input_path + '/scan_emtpy_lines.json', schema, None),
        conf=_enable_all_types_json_scan_conf)


@pytest.mark.parametrize('input_file', [
    pytest.param("int_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10458')),
    pytest.param("float_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10458')),
    pytest.param("sci_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10458')),
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    "invalid_ridealong_columns.json",
    pytest.param("int_array_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/rapidsai/cudf/issues/15318')),
    pytest.param("int_struct_formatted.json", marks=pytest.mark.xfail(condition=is_spark_400_or_later(), reason='https://github.com/NVIDIA/spark-rapids/issues/11154')),
    pytest.param("int_struct_formatted_problematic_rows.json", marks=pytest.mark.xfail(condition=is_spark_400_or_later(), reason='https://github.com/NVIDIA/spark-rapids/issues/11154')),
    pytest.param("int_mixed_array_struct_formatted.json", marks=pytest.mark.xfail(condition=is_spark_400_or_later(), reason='https://github.com/NVIDIA/spark-rapids/issues/11154')),
    "bad_whitespace.json",
    "escaped_strings.json",
    pytest.param("nested_escaped_strings.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11632')),
    pytest.param("repeated_columns.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11361')),
    pytest.param("mixed_objects.json", marks=pytest.mark.xfail(condition=is_spark_400_or_later(), reason='https://github.com/NVIDIA/spark-rapids/issues/11154')),
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json",
    "scan_emtpy_lines.json"])
@pytest.mark.parametrize('read_func', [read_json_df])
def test_scan_json_strings(std_input_path, read_func, spark_tmp_table_factory, input_file):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + input_file,
        StructType([StructField("data", StringType())]),
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

@pytest.mark.parametrize('input_file', [
    pytest.param("int_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10458')),
    pytest.param("float_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10458')),
    pytest.param("sci_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10458')),
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    "invalid_ridealong_columns.json",
    pytest.param("int_array_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/rapidsai/cudf/issues/15318')),
    "int_struct_formatted.json",
    "int_struct_formatted_problematic_rows.json",
    "int_mixed_array_struct_formatted.json",
    "bad_whitespace.json",
    "escaped_strings.json",
    pytest.param("nested_escaped_strings.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11632')),
    pytest.param("repeated_columns.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11361')),
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json"])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_strings(std_input_path, input_file):
    schema = StructType([StructField("data", StringType())])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

JSON_EXPRESSION_TEST_FILES = [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    "invalid_ridealong_columns.json",
    "int_array_formatted.json",
    "int_struct_formatted.json",
    "int_struct_formatted_problematic_rows.json",
    "int_mixed_array_struct_formatted.json",
    "bad_whitespace.json",
    "escaped_strings.json",
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json"]

def _get_json_object_formats(df):
    return df.selectExpr("*",
        '''get_json_object(json, "$.data")''',
        '''get_json_object(json, '$.id')''',
        '''get_json_object(json, '$.name')''')

def _get_json_object_child_formats(df):
    return df.selectExpr("*",
        '''get_json_object(json, "$.data.a")''',
        '''get_json_object(json, '$.tags[0]')''',
        '''get_json_object(json, '$.details.address.city')''',
        '''get_json_object(json, '$.user.profile.username')''',
        '''get_json_object(json, '$.user.skills[0]')''',
        '''get_json_object(json, '$.user.projects[1].name')''',
        '''get_json_object(json, '$.departments[0].employees[1].name')''',
        '''get_json_object(json, '$.departments[1].employees[0].id')''',
        '''get_json_object(json, '$.data.numeric')''',
        '''get_json_object(json, '$.data.details.timestamp')''',
        '''get_json_object(json, '$.data.details.list[1]')''',
        '''get_json_object(json, '$.company.departments[1].employees[0].name')''',
        '''get_json_object(json, '$.company.departments[0].employees[1].role')''')

def _json_tuple_formats(df):
    return df.selectExpr("*", '''json_tuple(json, "data")''').selectExpr("*",
        # json_tuple is not the same as get_json_object
        '''json_tuple(json, 'id', 'name', 'details.address.city') AS (id, name, city)''').selectExpr("*",
        '''json_tuple(json, 'user.profile.username', 'user.skills[0]', 'user.projects[1].name') AS (username, first_skill, second_project_name)''')

def _read_expression_json(spark, std_input_path, input_file):
    return read_json_as_text(spark, std_input_path + '/' + input_file, "json")

@pytest.mark.parametrize('input_file', JSON_EXPRESSION_TEST_FILES)
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_json_expression_formats(std_input_path, input_file):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: _json_tuple_formats(_get_json_object_child_formats(
            _get_json_object_formats(_read_expression_json(spark, std_input_path, input_file)))))

@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11387')
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_get_json_object_formats_nested_escaped(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: _get_json_object_formats(
            _read_expression_json(spark, std_input_path, "nested_escaped_strings.json")))

@allow_non_gpu(TEXT_INPUT_EXEC)
def test_get_json_object_child_formats_nested_escaped(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: _get_json_object_child_formats(
            _read_expression_json(spark, std_input_path, "nested_escaped_strings.json")))

@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11387')
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_json_tuple_formats_nested_escaped(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: _json_tuple_formats(
            _read_expression_json(spark, std_input_path, "nested_escaped_strings.json")))

@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11361')
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_get_json_object_formats_repeated_columns(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: _get_json_object_formats(
            _read_expression_json(spark, std_input_path, "repeated_columns.json")))

@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11361')
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_get_json_object_child_formats_repeated_columns(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: _get_json_object_child_formats(
            _read_expression_json(spark, std_input_path, "repeated_columns.json")))

@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11361')
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_json_tuple_formats_repeated_columns(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: _json_tuple_formats(
            _read_expression_json(spark, std_input_path, "repeated_columns.json")))

@approximate_float()
@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    pytest.param("float_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10481')),
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    "int_array_formatted.json",
    "int_struct_formatted.json",
    "int_struct_formatted_problematic_rows.json",
    "int_mixed_array_struct_formatted.json",
    "bad_whitespace.json",
    "escaped_strings.json",
    "nested_escaped_strings.json",
    pytest.param("repeated_columns.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11361')),
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json",
    "scan_emtpy_lines.json"])
@pytest.mark.parametrize('read_func', [read_json_df])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow)
def test_scan_json_floats(std_input_path, read_func, spark_tmp_table_factory, input_file):
    schema = StructType([StructField("data", FloatType())])
    if input_file == "scan_emtpy_lines.json":
        test_func = read_func(std_input_path + '/' + input_file, schema, spark_tmp_table_factory)
        conf = _enable_all_types_json_scan_conf
    else:
        test_func = lambda spark: _scan_and_from_json(
            spark, std_input_path + '/' + input_file, schema)
        conf = _enable_json_scan_and_to_structs_conf
    assert_gpu_and_cpu_are_equal_collect(
        test_func, conf=conf)

@approximate_float()
@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    pytest.param("float_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10481')),
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    "int_array_formatted.json",
    "int_struct_formatted.json",
    "int_struct_formatted_problematic_rows.json",
    "int_mixed_array_struct_formatted.json",
    "bad_whitespace.json",
    "escaped_strings.json",
    "nested_escaped_strings.json",
    pytest.param("repeated_columns.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11361')),
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json",
    "scan_emtpy_lines.json"])
@pytest.mark.parametrize('read_func', [read_json_df])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow)
def test_scan_json_doubles(std_input_path, read_func, spark_tmp_table_factory, input_file):
    schema = StructType([StructField("data", DoubleType())])
    if input_file == "scan_emtpy_lines.json":
        test_func = read_func(std_input_path + '/' + input_file, schema, spark_tmp_table_factory)
        conf = _enable_all_types_json_scan_conf
    else:
        test_func = lambda spark: _scan_and_from_json(
            spark, std_input_path + '/' + input_file, schema)
        conf = _enable_json_scan_and_to_structs_conf
    assert_gpu_and_cpu_are_equal_collect(
        test_func, conf=conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    pytest.param("int_formatted_strings.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/9664')),
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    "int_array_formatted.json",
    "int_struct_formatted.json",
    "int_struct_formatted_problematic_rows.json",
    "int_mixed_array_struct_formatted.json",
    "bad_whitespace.json",
    pytest.param("escaped_strings.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/9664')),
    "nested_escaped_strings.json",
    pytest.param("repeated_columns.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11361')),
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json",
    "scan_emtpy_lines.json"])
@pytest.mark.parametrize('read_func', [read_json_df])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_scan_json_corrected_dates(std_input_path, read_func, spark_tmp_table_factory, input_file):
    schema = StructType([StructField("data", DateType())])
    conf = copy_and_update(_enable_json_scan_and_to_structs_conf,
        {"spark.sql.legacy.timeParserPolicy": "CORRECTED",
         "spark.rapids.sql.json.read.datetime.enabled": "true"})
    if input_file == "scan_emtpy_lines.json":
        test_func = read_func(std_input_path + '/' + input_file, schema, spark_tmp_table_factory)
    else:
        test_func = lambda spark: _scan_and_from_json(
            spark, std_input_path + '/' + input_file, schema)
    assert_gpu_and_cpu_are_equal_collect(
        test_func, conf=conf)

@pytest.mark.parametrize('input_file', [
    pytest.param("int_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10535')),
    "float_formatted.json",
    "sci_formatted.json",
    pytest.param("int_formatted_strings.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10535')),
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    pytest.param("decimal_locale_formatted_strings.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10535')),
    "single_quoted_strings.json",
    pytest.param("boolean_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10535')),
    "int_array_formatted.json",
    "int_struct_formatted.json",
    "int_struct_formatted_problematic_rows.json",
    "int_mixed_array_struct_formatted.json",
    "bad_whitespace.json",
    "escaped_strings.json",
    "nested_escaped_strings.json",
    pytest.param("repeated_columns.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11361')),
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    pytest.param("timestamp_tz_formatted_strings.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/6846')),
    "scan_emtpy_lines.json"])
@pytest.mark.parametrize('read_func', [read_json_df])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow)
def test_scan_json_corrected_timestamps(std_input_path, read_func, spark_tmp_table_factory, input_file):
    schema = StructType([StructField("data", TimestampType())])
    conf = copy_and_update(_enable_json_scan_and_to_structs_conf,
        {"spark.sql.legacy.timeParserPolicy": "CORRECTED",
         "spark.rapids.sql.json.read.datetime.enabled": "true"})
    if input_file == "scan_emtpy_lines.json":
        test_func = read_func(std_input_path + '/' + input_file, schema, spark_tmp_table_factory)
    else:
        test_func = lambda spark: _scan_and_from_json(
            spark, std_input_path + '/' + input_file, schema)
    assert_gpu_and_cpu_are_equal_collect(
        test_func, conf=conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    pytest.param("int_array_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10573')),
    "int_struct_formatted.json",
    "int_struct_formatted_problematic_rows.json",
    pytest.param("int_mixed_array_struct_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11491')),
    "bad_whitespace.json",
    "escaped_strings.json",
    "nested_escaped_strings.json",
    "repeated_columns.json",
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json",
    "scan_emtpy_lines.json"])
@pytest.mark.parametrize('read_func', [read_json_df]) # we have done so many tests already that we don't need both read func. They are the same
def test_scan_json_long_arrays(std_input_path, read_func, spark_tmp_table_factory, input_file):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + input_file,
        StructType([StructField("data", ArrayType(LongType()))]),
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    pytest.param("int_array_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10573')),
    "int_struct_formatted.json",
    "int_struct_formatted_problematic_rows.json",
    pytest.param("int_mixed_array_struct_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11491')),
    "bad_whitespace.json",
    "escaped_strings.json",
    "nested_escaped_strings.json",
    "repeated_columns.json",
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json"])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_long_arrays(std_input_path, input_file):
    schema = StructType([StructField("data", ArrayType(LongType()))])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    pytest.param("int_array_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10574')),
    "int_struct_formatted.json",
    "int_struct_formatted_problematic_rows.json",
    "int_mixed_array_struct_formatted.json",
    "bad_whitespace.json",
    "escaped_strings.json",
    "nested_escaped_strings.json",
    "repeated_columns.json",
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json",
    "scan_emtpy_lines.json"])
@pytest.mark.parametrize('read_func', [read_json_df]) # we have done so many tests already that we don't need both read func. They are the same
def test_scan_json_string_arrays(std_input_path, read_func, spark_tmp_table_factory, input_file):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + input_file,
        StructType([StructField("data", ArrayType(StringType()))]),
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    pytest.param("int_array_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10574')),
    "int_struct_formatted.json",
    "int_struct_formatted_problematic_rows.json",
    "int_mixed_array_struct_formatted.json",
    "bad_whitespace.json",
    "escaped_strings.json",
    "nested_escaped_strings.json",
    "repeated_columns.json",
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json"])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_string_arrays(std_input_path, input_file):
    schema = StructType([StructField("data", ArrayType(StringType()))])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    "int_array_formatted.json",
    "int_struct_formatted.json",
    pytest.param("int_struct_formatted_problematic_rows.json", marks=pytest.mark.xfail(
        condition=is_before_spark_342() or is_databricks_version_or_later(14, 3),
        reason='Before Spark 3.4.2? https://github.com/NVIDIA/spark-rapids/issues/10588. '
               'Databricks 14.3 or later? https://github.com/NVIDIA/spark-rapids/issues/11711.')),
    pytest.param("int_mixed_array_struct_formatted.json", marks=pytest.mark.xfail(condition=is_before_spark_342(),reason='https://github.com/NVIDIA/spark-rapids/issues/10588')),
    "bad_whitespace.json",
    "escaped_strings.json",
    "nested_escaped_strings.json",
    pytest.param("repeated_columns.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11361')),
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json",
    "scan_emtpy_lines.json"])
@pytest.mark.parametrize('read_func', [read_json_df]) # we have done so many tests already that we don't need both read func. They are the same
@pytest.mark.xfail(condition=is_databricks_runtime(), reason='https://github.com/NVIDIA/spark-rapids/issues/11711')
def test_scan_json_long_structs(std_input_path, read_func, spark_tmp_table_factory, input_file):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + input_file,
        StructType([StructField("data", StructType([StructField("A", LongType()),StructField("B", LongType())]))]),
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    "int_array_formatted.json",
    "int_struct_formatted.json",
    pytest.param("int_struct_formatted_problematic_rows.json", marks=pytest.mark.xfail(
        condition=is_before_spark_342() or is_databricks_version_or_later(14, 3),
        reason='Before Spark 3.4.2? https://github.com/NVIDIA/spark-rapids/issues/10588. '
               'Databricks 14.3 or later? https://github.com/NVIDIA/spark-rapids/issues/11711.')),
    pytest.param("int_mixed_array_struct_formatted.json", marks=pytest.mark.xfail(condition=is_before_spark_342(),reason='https://github.com/NVIDIA/spark-rapids/issues/10588')),
    "bad_whitespace.json",
    "escaped_strings.json",
    "nested_escaped_strings.json",
    pytest.param("repeated_columns.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11361')),
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json"])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
@pytest.mark.xfail(condition=is_databricks_runtime(), reason='https://github.com/NVIDIA/spark-rapids/issues/11711')
def test_from_json_long_structs(std_input_path, input_file):
    schema = StructType([StructField("data", StructType([StructField("A", LongType()),StructField("B", LongType())]))])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    "int_array_formatted.json",
    "int_struct_formatted.json",
    "int_struct_formatted_problematic_rows.json",
    "int_mixed_array_struct_formatted.json",
    "bad_whitespace.json",
    "escaped_strings.json",
    "nested_escaped_strings.json",
    pytest.param("repeated_columns.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11361')),
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json",
    "scan_emtpy_lines.json"])
@pytest.mark.parametrize('read_func', [read_json_df]) # we have done so many tests already that we don't need both read func. They are the same
def test_scan_json_string_structs(std_input_path, read_func, spark_tmp_table_factory, input_file):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + input_file,
        StructType([StructField("data", StructType([StructField("A", StringType()),StructField("B", StringType())]))]),
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    "int_array_formatted.json",
    "int_struct_formatted.json",
    "int_struct_formatted_problematic_rows.json",
    "int_mixed_array_struct_formatted.json",
    "bad_whitespace.json",
    "escaped_strings.json",
    "nested_escaped_strings.json",
    pytest.param("repeated_columns.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11361')),
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json"])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_string_structs(std_input_path, input_file):
    schema = StructType([StructField("data", StructType([StructField("A", StringType()),StructField("B", StringType())]))])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

@pytest.mark.parametrize('dt', [DecimalType(38,0), DecimalType(10,2)], ids=idfn)
@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    pytest.param("int_array_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10573')), # This does not fail on 38,0
    "int_struct_formatted.json",
    "int_struct_formatted_problematic_rows.json",
    pytest.param("int_mixed_array_struct_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11491')),
    "bad_whitespace.json",
    "escaped_strings.json",
    "repeated_columns.json",
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json",
    "scan_emtpy_lines.json"])
@pytest.mark.parametrize('read_func', [read_json_df]) # we have done so many tests already that we don't need both read func. They are the same
def test_scan_json_dec_arrays(std_input_path, read_func, spark_tmp_table_factory, input_file, dt):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + input_file,
        StructType([StructField("data", ArrayType(dt))]),
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

@pytest.mark.parametrize('dt', [DecimalType(38,0), DecimalType(10,2)], ids=idfn)
@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    pytest.param("int_array_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10573')), # This does not fail on 38,0
    "int_struct_formatted.json",
    "int_struct_formatted_problematic_rows.json",
    pytest.param("int_mixed_array_struct_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11491')),
    "bad_whitespace.json",
    "escaped_strings.json",
    "nested_escaped_strings.json",
    "repeated_columns.json",
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json"])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_dec_arrays(std_input_path, input_file, dt):
    schema = StructType([StructField("data", ArrayType(dt))])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json",
    "int_array_formatted.json",
    "int_struct_formatted.json",
    "int_struct_formatted_problematic_rows.json",
    "int_mixed_array_struct_formatted.json",
    "bad_whitespace.json",
    "escaped_strings.json",
    "nested_escaped_strings.json",
    pytest.param("repeated_columns.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/11361')),
    "mixed_objects.json",
    "timestamp_formatted_strings.json",
    "timestamp_tz_formatted_strings.json",
    "scan_emtpy_lines.json"])
@pytest.mark.parametrize('read_func', [read_json_df]) # we have done so many tests already that we don't need both read func. They are the same
def test_scan_json_mixed_struct(std_input_path, read_func, spark_tmp_table_factory, input_file):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + input_file,
        StructType([StructField("data", StructType([StructField("A", StringType()),StructField("B", StringType())]))]),
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

@pytest.mark.parametrize('input_file, schema', [
    ("mixed_objects.json", "id INT, name STRING, tags ARRAY<STRING>, details STRUCT<age: INT, address: STRUCT<city: STRING, zip: STRING>>"),
    ("mixed_objects.json", "user STRUCT<profile: STRUCT<username: STRING, email: STRING>, skills: ARRAY<STRING>, projects: ARRAY<STRUCT<name: STRING, status: STRING>>>"),
    ("mixed_objects.json", "departments ARRAY<STRUCT<name: STRING, employees: ARRAY<STRUCT<id: INT, name: STRING>>>>"),
    ("mixed_objects.json", "data STRUCT<numeric: INT, text: STRING, flag: BOOLEAN, details: STRUCT<timestamp: STRING, list: ARRAY<INT>>>"),
    ("mixed_objects.json", "data STRUCT<numeric: INT, text: STRING, flag: BOOLEAN, details: STRUCT<timestamp: TIMESTAMP, list: ARRAY<INT>>>"),
    ("mixed_objects.json", "data STRUCT<numeric: INT, text: STRING, flag: BOOLEAN, details: STRUCT<timestamp: DATE, list: ARRAY<INT>>>"),
    ("mixed_objects.json", "company STRUCT<departments: ARRAY<STRUCT<department_name: STRING, employees: ARRAY<STRUCT<name: STRING, role: STRING>>>>>"),
    ("mixed_objects.json", "MAP<STRING,STRING>")
    ])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_mixed_corrected(std_input_path, input_file, schema):
    conf = copy_and_update(_enable_json_to_structs_conf, 
        {"spark.sql.legacy.timeParserPolicy": "CORRECTED",
         "spark.rapids.sql.json.read.datetime.enabled": "true"})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").selectExpr('json',
            "from_json(json, '" + schema + "') as parsed"),
        conf = conf)
