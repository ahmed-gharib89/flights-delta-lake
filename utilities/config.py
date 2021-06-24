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
import os
import pandas as pd
from glob import glob
import logging
from IPython.core.display import HTML
from utilities.flights_raw import FlightsRaw
from utilities.operations import(
    create_stream_writer,
    read_stream_delta,
    read_stream_raw,
    stop_all_streams,
    stop_named_stream,
    until_stream_is_ready,
    register_delta_table,
    transform_raw
)

print('importing libraries ....')
print(
    """
Libraries (
    os, pandas as pd, glob, psutils, logging, findspark,
    pyspark.sql.SparkSession, delta.configure_spark_with_delta_pip,
    HTML, FlightsRaw, create_stream_writer, read_stream_delta,
    read_stream_raw, stop_all_streams, stop_named_stream, 
    until_stream_is_ready, register_delta_table, transform_raw
)
Are available now
"""
)

# Set pandas maximum column width to 400
pd.options.display.max_colwidth = 400
# Setting pandas to display all columns
pd.options.display.max_columns = None
print('pandas maximum column width is set to 400 and maximum number of columns to None')
logging.info('pandas maximum column width is set to 400 and maximum number of columns to None')
print('Setting up variables ....')
logging.info('Setting up variables ....')
# Setting the path for data source and delta lake
working_dir = os.getcwd()
data_source = os.path.join(working_dir, 'data_source')
delta_lake = os.path.join(working_dir, 'delta_lake')
flight_raw_path = os.path.join(delta_lake, 'flights_raw')
# Creating the flight raw directory if not exists
if not os.path.isdir(flight_raw_path):
    os.mkdir(flight_raw_path)
flight_bronz_path = os.path.join(delta_lake, 'flight_bronz')
flight_silver_path = os.path.join(delta_lake, 'flight_silver')
flight_gold_path = os.path.join(delta_lake, 'flight_gold')
checkpoints_path = os.path.join(working_dir, 'checkpoints')
flight_bronz_checkpoint = os.path.join(checkpoints_path, "flight_bronz")
flight_silver_checkpoint = os.path.join(checkpoints_path, "flight_silver")
flight_gold_checkpoint = os.path.join(checkpoints_path, "flight_gold")



# Vriables list of dictionaries to organize and print the variables when loading the configurations
vars = [
    {'Name': 'working_dir', 'Value': working_dir,
        'Description': 'string path for current working directory'},
    {'Name': 'data_source', 'Value': data_source,
        'Description': 'string path for data source location'},
    {'Name': 'delta_lake', 'Value': delta_lake,
        'Description': 'string path for delta lake location'},
    {'Name': 'flight_raw_path', 'Value': flight_raw_path,
        'Description': 'string path for flight raw data'},
    {'Name': 'flight_bronz_path', 'Value': flight_bronz_path,
        'Description': 'string path for flight bronz data'},
    {'Name': 'flight_silver_path', 'Value': flight_silver_path,
        'Description': 'string path for flight silver data'},
    {'Name': 'flight_gold_path', 'Value': flight_gold_path,
        'Description': 'string path for flight gold data'},
    {'Name': 'checkpoints_path', 'Value': checkpoints_path,
        'Description': 'string path for checkpoints directory'},
    {'Name': 'flight_bronz_checkpoint', 'Value': flight_bronz_checkpoint,
        'Description': 'string path for flight bronz checkpoint'},
    {'Name': 'flight_silver_checkpoint', 'Value': flight_silver_checkpoint,
        'Description': 'string path for flight silver checkpoint'},
    {'Name': 'flight_gold_checkpoint', 'Value': flight_gold_checkpoint,
        'Description': 'string path for flight gold checkpoint'}
]

vars_df = pd.DataFrame(vars).to_html()
print('vars_df is available as HTML content to display simply run HTML(vars_df)')
# logging.info('vars_df is available as HTML content to display simply run HTML(vars_df)')
