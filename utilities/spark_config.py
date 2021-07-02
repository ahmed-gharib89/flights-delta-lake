#!/c/Users/aghar/anaconda3/envs/ds/python
# -*- coding: utf-8 -*-
#
# PROGRAMMER: Ahmed Gharib
# DATE CREATED: 20/06/2021
# REVISED DATE:
# PURPOSE: Configuration file to hold all the variables and import needed for DEND project
#
##
# Imports python modules
import pandas as pd
import psutil
import logging
from IPython.core.display import HTML
import findspark
findspark.init()
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from utilities.operations import(
    create_stream_writer,
    read_stream_delta,
    read_stream_raw,
    stop_all_streams,
    stop_named_stream,
    until_stream_is_ready,
    register_delta_table,
    transform_raw,
    get_flight_schema,
    transform_flight_bronz,
    create_or_update_date_table,
    create_or_update_flight_gold,
    load_csv_to_dataframe,
    transform_lookup_airport,
    transform_lookup_plane,
    add_comments_to_table
)
from utilities.explore_dfs import ExploreDfs

print('importing libraries ....')
print(
    """
Libraries (
    pandas as pd, psutils, logging, findspark,
    pyspark.sql.SparkSession, delta.configure_spark_with_delta_pip,
    HTML, create_stream_writer, read_stream_delta, read_stream_raw,
    stop_all_streams, stop_named_stream, until_stream_is_ready,
    register_delta_table, transform_raw, get_flight_schema,
    create_or_update_date_table, load_csv_to_dataframe,
    transform_lookup_airport, transform_lookup_plane, ExploreDfs
)
Are available now
"""
)


print('Setting up spark configurations.....')
logging.info('Setting up spark configurations.....')
# Setting spark configurations
# Number of cpu cores to be used as the number of shuffle partitions
num_cpus = psutil.cpu_count()
# Spark UI port
spark_ui_port = '4050'
# Set offHeap Size to 10 GB
offHeap_size = str(10 * 1024 * 1024 * 1024)
# Spark Application Name
spark_app_name = 'lake-house'
# Builder for spark configurations
builder = (
    SparkSession.builder.appName(spark_app_name)
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    .config('spark.ui.port', spark_ui_port)
    .config('spark.sql.shuffle.partitions', num_cpus)
    .config('spark.sql.adaptive.enabled', True)
    .config('spark.memory.offHeap.enabled', True)
    .config('spark.memory.offHeap.size', offHeap_size)
    .enableHiveSupport()
)
# configure spark to use open source delta
spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark_configurations = [
    {'Config': 'spark.sql.extensions', 'Value': 'io.delta.sql.DeltaSparkSessionExtension',
        'Description': 'Using delta io extension'},
    {'Config': 'spark.sql.catalog.spark_catalog', 'Value': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
        'Description': 'Setting spark catalog to use DeltaCatalog'},
    {'Config': 'spark.ui.port', 'Value': spark_ui_port,
        'Description': 'Spark UI port number'},
    {'Config': 'spark.sql.shuffle.partitions', 'Value': num_cpus,
        'Description': 'setting the number of shuffle partitions to the number of cores available'},
    {'Config': 'spark.sql.adaptive.enabled', 'Value': True,
        'Description': 'Enabling adaptive query optimization'},
    {'Config': 'spark.memory.offHeap.enabled', 'Value': True,
        'Description': 'Enabling offHeap memory'},
    {'Config': 'spark.memory.offHeap.size', 'Value': offHeap_size,
        'Description': 'Setting offHeap memory to 10 GB'}
]

spark_config_df = pd.DataFrame(spark_configurations).to_html()
print('spark session is now available in the environment as spark\n\
spark_config_df is available as HTML content to display simply run HTML(spark_config_df)')

# Setting new DB to use
spark.sql("""
    CREATE DATABASE IF NOT EXISTS flights_db
"""
)

spark.sql("""
    USE flights_db
"""
)
print("Using flights_db database..")
