#!/c/Users/aghar/anaconda3/envs/ds/python
# -*- coding: utf-8 -*-
#
# PROGRAMMER: Ahmed Gharib
# DATE CREATED: 22/06/2021
# REVISED DATE:
# PURPOSE: Helper ETL functions for building US Immigrations Data Lake-house
#
##
# Imports python modules
import time
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    mean,
    stddev,
    max,
)


def create_stream_writer(
    dataframe,
    checkpoint,
    name,
    partition_column=None,
    mode="append",
):
    """Creates a spark stream writer

    Args:
        dataframe (DataFrame): spark dataframe to stream from
        checkpoint (str): stream checkpoint path
        name (str): stream name
        partition_column (str, optional): column to partition by. Defaults to None.
        mode (str, optional): output mode. Defaults to "append".

    Returns:
        StreamWriter: spark stream writer
    """

    stream_writer = (
        dataframe.writeStream.format("delta")
        .outputMode(mode)
        .option("checkpointLocation", checkpoint)
        .queryName(name)
    )

    if partition_column is not None:
        stream_writer = stream_writer.partitionBy(partition_column)

    return stream_writer

def read_stream_delta(spark, delta_path):
    """Read stream from delta path

    Args:
        spark (SparkSession): spark session 
        delta_path (str): string path for delta table directory

    Returns:
        DataFrame: Spark stream dataframe
    """
    return spark.readStream.format("delta").load(delta_path)

def read_stream_raw(spark, raw_path, schema):
    """Read stream from raw data directory

    Args:
        spark (SparkSession): spark session 
        raw_path (str): string path for raw data directory
        schema (str): Schema to be used as dataframe schema

    Returns:
        DataFrame: Spark stream dataframe
    """
    return spark.readStream.format("csv").option("header", "true").schema(schema).load(raw_path)

def stop_all_streams(spark):
    """Stops all streams

    Args:
        spark (SparkSession): spark session 

    Returns:
        bool: True if all streams stopped
    """
    stopped = False
    for stream in spark.streams.active:
        stopped = True
        stream.stop()
    return stopped

def stop_named_stream(spark, named_stream):
    """Stop specific stream by its name

    Args:
        spark (SparkSession): spark session 
        named_stream (str): Stream name

    Returns:
        bool: True if the stream stopped
    """
    stopped = False
    for stream in spark.streams.active:
        if stream.name == named_stream:
            stopped = True
            stream.stop()
    return stopped

def until_stream_is_ready(spark, named_stream, progressions=3, wait_for=60):
    """Checks for stream to and wait until it is ready 

    Args:
        spark (SparkSession): spark session 
        named_stream (str): Stream name
        progressions (int, optional): stream progress threshold to be sure that it's active. Defaults to 3.

    Returns:
        bool: True right after the stream is active and ready
    """
    slept_for = 0
    queries = list(filter(lambda query: query.name == named_stream, spark.streams.active))
    while len(queries) == 0 or len(queries[0].recentProgress) < progressions:
        time.sleep(5)
        slept_for += 5
        queries = list(filter(lambda query: query.name == named_stream, spark.streams.active))
        if slept_for >= wait_for:
            print(f"Timed out.")
            return False
    print(f"The stream {named_stream} is active and ready.")
    return True

def register_delta_table(spark, table_name, delta_path):
    """Registers delta table to metastore

    Args:
        spark (SparkSession): spark session 
        table_name (str): Table name
        delta_path (str): string path for delta table directory
    """
    delta_location = '/' + delta_path.replace('\\', '/')
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"""
        CREATE TABLE {table_name}
        USING DELTA
        LOCATION "{delta_location}"
    """
    )

def transform_raw(dataframe, source_location, partition_column=None):
    """Adds metadata to the raw dataframe

    Args:
        dataframe (DataFrame): Spark dataframe holding the raw dataframe
        source_location (str): string for location of raw data
        partition_column (str, optional): column to partition by. Defaults to None.

    Returns:
        dataframe (DataFrame): transformed dataframe after adding metadata
    """
    dataframe = dataframe.select(
        '*',
        lit(source_location).alias('data_source'),
        current_timestamp().alias('ingest_time'),
        current_timestamp().cast('date').alias('ingest_date')
    )
    # Prefixing the partition column with p_ to let anyone use the table know that it's partiotioned by this column
    if partition_column:
        dataframe = dataframe.withColumnRenamed(partition_column, 'p_' + partition_column)

    return dataframe
