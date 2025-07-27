from chispa.dataframe_comparer import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

from ..jobs.actors_history_scd_job import do_actor_history_scd_transformation
from collections import namedtuple

history = namedtuple("history",  "actor_id actor quality_class is_active current_year")
# Define the schema for the expected DataFrame explicitly to match the output of the SQL query
expected_schema = StructType([
    StructField("actor_id", StringType(), True),
    StructField("actor", StringType(), True),
    StructField("start_date", IntegerType(), True),
    StructField("end_date", IntegerType(), True),
    StructField("quality_class", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("current_year", IntegerType(), False)
])

# Define the schema for the source DataFrame explicitly
source_schema = StructType([
    StructField("actor_id", StringType(), True),
    StructField("actor", StringType(), True),
    StructField("quality_class", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("current_year", IntegerType(), True)
])


def test_scd_generation(spark):
    source_data = [
        history("nm0000001", "Fred Astaire", 'average', True, 2020),
        history("nm0000001", "Fred Astaire", 'average', True, 2021),
        history("nm0000001", "Fred Astaire", 'good', True, 2022),
        history("nm0000002", "Fred Astaire", 'average', True, 2020),
        history("nm0000002", "Fred Astaire", 'good', True, 2021)
    ]
    # Create source_df with the explicitly defined schema
    source_df = spark.createDataFrame(source_data, schema=source_schema)

    actual_df = do_actor_history_scd_transformation(spark, source_df)
    expected_data = [
        ("nm0000001", "Fred Astaire", 2020, 2021, 'average', True, 2021),
        ("nm0000001", "Fred Astaire", 2022, 2022, 'good', True, 2021),
        ("nm0000002", "Fred Astaire", 2020, 2020, 'average', True, 2021),
        ("nm0000002", "Fred Astaire", 2021, 2021, 'good', True, 2021)
    ]
    # Create expected_df with the explicitly defined schema
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    assert_df_equality(actual_df, expected_df)
