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
import pandas as pd
import pyspark.sql.functions  as F


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
        F.lit(source_location).alias('data_source'),
        F.current_timestamp().alias('ingest_time'),
        F.current_timestamp().cast('date').alias('ingest_date')
    )
    # Prefixing the partition column with p_ to let anyone use the table know that it's partiotioned by this column
    if partition_column:
        dataframe = dataframe.withColumnRenamed(partition_column, 'p_' + partition_column)

    return dataframe

def get_flight_schema(spark, flisghts_raw):
    """Gets flights schema from the first retrived file in flights_raw

    Args:
        spark (SparkSession): spark session 
        flisghts_raw (FlightsRaw): FlightsRaw class availble in flights_raw.py

    Returns:
        schema: spak schema object
    """
    dataframe = load_csv_to_dataframe(spark, flisghts_raw.retrived_files[0])
    schema = dataframe.schema
    return schema

def load_csv_to_dataframe(spark, path):
    """Loads csv file into spark dataframe

    Args:
        spark (SparkSession): spark session 
        path (str): string path for the csv file to be loaded

    Returns:
        dataframe: spark dataframe
    """
    dataframe = (
        spark.read.format('csv')
        .option('header', 'true')
        .option('inferSchema', 'true')
        .load(path)
    )
    
    return dataframe
    
def transform_flight_bronz(dataframe):
    """Transforms flight bronz data frame and prepare it to be writen to silver table

    Args:
        dataframe (DataFrame): spark dataframe to be transformed

    Returns:
        dataframe (DataFrame): spark dataframe after being transformed
    """
    dataframe = _construct_date(dataframe)
    cols_to_convert = ["DepTime", "CRSDepTime", "ArrTime", "CRSArrTime"]
    dataframe = _convert_number_time(dataframe, cols_to_convert)
    dataframe = (
        dataframe.select(
            'p_Year',
            'date', 
            'DepTime', 
            'CRSDepTime', 
            'ArrTime',
            'CRSArrTime', 
            'UniqueCarrier', 
            'FlightNum', 
            'TailNum', 
            F.col('ActualElapsedTime').cast('integer').alias('ActualElapsedTime'),
            F.col('CRSElapsedTime').cast('integer').alias('CRSElapsedTime'),
            F.col('AirTime').cast('integer').alias('AirTime'),
            F.col('ArrDelay').cast('integer').alias('ArrDelay'),
            F.col('DepDelay').cast('integer').alias('DepDelay'),
            'Origin', 
            'Dest',
            F.col('Distance').cast('integer').alias('Distance'),
            F.col('TaxiIn').cast('integer').alias('TaxiIn'),
            F.col('TaxiOut').cast('integer').alias('TaxiOut'),
            F.col('Cancelled').cast('boolean').alias('Cancelled'),
            'CancellationCode',
            F.col('Diverted').cast('boolean').alias('Diverted'),
            F.col('CarrierDelay').cast('integer').alias('CarrierDelay'),
            F.col('WeatherDelay').cast('integer').alias('WeatherDelay'),
            F.col('NASDelay').cast('integer').alias('NASDelay'),
            F.col('SecurityDelay').cast('integer').alias('SecurityDelay'),
            F.col('LateAircraftDelay').cast('integer').alias('LateAircraftDelay')
        )
    )
    return dataframe


def _construct_date(dataframe):
    """Constructs data column from year, month and day

    Args:
        dataframe (DataFrame): spark dataframe to add the date column to

    Returns:
         dataframe (DataFrame): spark dataframe after adding the date column
    """
    dataframe = (
        dataframe.withColumn(
            "date", 
            F.concat_ws("-", F.col("p_Year"), F.col("Month"), F.col("DayofMonth"))
            .cast("date"))
    )
    return dataframe

def _convert_number_time(dataframe, cols_to_convert):
    """Converts list of columns to HH:mm format instead of numeric value

    Args:
        dataframe (DataFrame): spark dataframe to transform
        cols_to_convert (list): list of column names to convert

    Returns:
        dataframe: spark dataframe after adding the date column
    """
    # Replace 2400 with 0 to be able to transform it to time stamp
    dataframe = dataframe.replace(2400, 0, subset=cols_to_convert)
    for col_name in cols_to_convert:
        dataframe = (
            dataframe
            .withColumn(col_name, F.col(col_name).cast("integer").cast("string"))
            .withColumn(col_name, F.lpad(F.col(col_name),4,'0'))
            .withColumn(col_name, F.from_unixtime(F.unix_timestamp(F.col(col_name), "HHmm"),"HH:mm"))
        )
    return dataframe

def add_comments_to_table(spark, table, col_comment_dict):
    """Add comments to table metadata

    Args:
        spark (SparkSession): spark session 
        table (str): table name to add comments to its metadata
        col_comment_dict (dictionary): dictionary with column names as key and comments as values
    """
    for col_name, comment in col_comment_dict.items():
        spark.sql(
            f"""
                ALTER TABLE {table} CHANGE COLUMN {col_name} COMMENT '{comment}'
            """)

def create_or_update_flight_gold(spark, flight_silver_table, flight_gold_table, flight_gold_bath):
    """Create summary table aggrigated by date

    Args:
        spark (SparkSession): spark session 
        flight_silver_table (str): name of flight silver table
        flight_gold_table (str): name of flight gold table
    """
    # spark.sql(f"""
    #     DROP TABLE IF EXISTS {flight_gold_table}
    # """)

    dataframe = spark.sql(f"""
            SELECT 
                date,
                COUNT(DISTINCT UniqueCarrier) AS num_carriers,
                SUM(CAST(Cancelled AS INTEGER)) AS total_cancelled,
                SUM(CAST(Diverted AS INTEGER))AS total_diverted,
                ROUND(AVG(AirTime), 2) AS avg_air_time,
                SUM(AirTime) AS total_air_time,
                SUM(CASE WHEN DepDelay <= 0 OR DepDelay IS NULL THEN 0 ELSE 1 END) AS count_dep_delay,  
                SUM(CASE WHEN DepDelay < 0 OR DepDelay IS NULL THEN 0 ELSE DepDelay END) AS total_dep_delay,  
                ROUND(AVG(CASE WHEN DepDelay <= 0 THEN NULL ELSE DepDelay END), 2) AS average_dep_delay,
                SUM(CASE WHEN ArrDelay <= 0 OR ArrDelay IS NULL THEN 0 ELSE 1 END) AS count_arr_delay,  
                SUM(CASE WHEN ArrDelay < 0 OR ArrDelay IS NULL THEN 0 ELSE ArrDelay END) AS total_arr_delay,  
                ROUND(AVG(CASE WHEN ArrDelay <= 0 THEN NULL ELSE ArrDelay END), 2) AS average_arr_delay,
                SUM(CASE WHEN CarrierDelay <= 0 OR CarrierDelay IS NULL THEN 0 ELSE 1 END) AS count_carrier_delay,  
                SUM(CASE WHEN CarrierDelay IS NULL THEN 0 ELSE CarrierDelay END) AS total_carrier_delay,
                ROUND(AVG(CASE WHEN CarrierDelay <= 0 THEN NULL ELSE CarrierDelay END), 2) AS average_carrier_delay,
                SUM(CASE WHEN WeatherDelay <= 0 OR WeatherDelay IS NULL THEN 0 ELSE 1 END) AS count_weather_delay,  
                SUM(CASE WHEN WeatherDelay IS NULL THEN 0 ELSE WeatherDelay END) AS total_weather_delay,
                ROUND(AVG(CASE WHEN WeatherDelay <= 0 THEN NULL ELSE WeatherDelay END), 2) AS average_weather_delay,
                SUM(CASE WHEN NASDelay <= 0 OR NASDelay IS NULL THEN 0 ELSE 1 END) AS count_nas_delay,  
                SUM(CASE WHEN NASDelay IS NULL THEN 0 ELSE NASDelay END) AS total_nas_delay,
                ROUND(AVG(CASE WHEN NASDelay <= 0 THEN NULL ELSE NASDelay END), 2) AS average_nas_delay,
                SUM(CASE WHEN SecurityDelay <= 0 OR SecurityDelay IS NULL THEN 0 ELSE 1 END) AS count_security_delay,  
                SUM(CASE WHEN SecurityDelay IS NULL THEN 0 ELSE SecurityDelay END) AS total_security_delay,
                ROUND(AVG(CASE WHEN SecurityDelay <= 0 THEN NULL ELSE SecurityDelay END), 2) AS average_security_delay,
                SUM(CASE WHEN LateAircraftDelay <= 0 OR LateAircraftDelay IS NULL THEN 0 ELSE 1 END) AS count_aircraft_delay,  
                SUM(CASE WHEN LateAircraftDelay IS NULL THEN 0 ELSE LateAircraftDelay END) AS total_aircraft_delay,
                ROUND(AVG(CASE WHEN LateAircraftDelay <= 0 THEN NULL ELSE LateAircraftDelay END), 2) AS average_aircraft_delay,
                SUM(Distance) AS total_distance,
                ROUND(AVG(Distance), 2) AS average_distance,
                COUNT(1)AS num_flights
            FROM
                {flight_silver_table}
            GROUP BY
                date
            ORDER BY
                date
    """)
    print(f'Saving {flight_gold_table} table to {flight_gold_bath}')
    dataframe.write.format('delta').save(flight_gold_bath)
    print(f'Registering date table with name {flight_gold_table} in the metastore')
    register_delta_table(spark, flight_gold_table, flight_gold_bath)
    print('Done...')

def create_or_update_date_table(spark, table, dim_date_table_name, dim_date_location_path):
    """Creates or update diminsion date table by generating full years
    from minimum year found in date colum in the table to the maximum

    Args:
        spark (SparkSession): spark session 
        table (str): name of the table to get our min and max year
        dim_date_table_name (str): dim date table name to register it to the meta store
        dim_date_lication_path (str): path to the location on disk to save the delta table
    Returns:
        dateframe: spark dataframe with 
    """
    query = f"""
                SELECT 
                    MIN(date) as min_date,
                    MAX(date) as max_date 
                FROM
                    {table}
            """
    min_max_date_dict = spark.sql(query).collect()[0].asDict()
    min_year = min_max_date_dict['min_date'].year
    max_year = min_max_date_dict['max_date'].year
    start_date = pd.to_datetime(f'{1}{min_year}', format='%m%Y')
    end_date = pd.to_datetime(f'{3112}{max_year}', format='%d%m%Y')
    print(f'Generating pandas dataframe for dates from {start_date.date()} to {end_date.date()}')
    pdf = pd.DataFrame({'dte': pd.date_range(start_date, end_date)})
    pdf.dte = pdf.dte.astype('str')
    print('Converting pandas df to spark df and adding more features')
    date_df = spark.createDataFrame(pdf)
    date_df = (
        date_df
        .withColumn('dte', F.to_date('dte'))
        .withColumn('year', F.year('dte'))
        .withColumn('quarter', F.concat_ws('Q', F.lit(''), F.quarter('dte')))
        .withColumn('month', F.month('dte'))
        .withColumn('day', F.dayofmonth('dte'))
        .withColumn('dayofweek', F.dayofweek('dte'))
        .withColumn('dayofyear', F.dayofyear('dte'))
        .withColumn('weekofyear', F.weekofyear('dte'))
        .withColumn('month_short', F.date_format('dte', 'MMM'))
        .withColumn('month_name', F.date_format('dte', 'MMMM'))
        .withColumn('month_year', F.concat_ws('-', F.col('year'), F.col('month_short')))
        .withColumn('sort_month_year', F.col('year') * 100 + F.col('month'))
        .withColumn('quarter_year', F.concat_ws('-', F.col('year'), F.col('quarter')))
        .withColumn('sort_quarter_year', F.col('year') * 100 + F.quarter('dte'))
    )
    print(f'Saving dim date delta table to {dim_date_location_path}')
    date_df.write.format('delta').mode('overwrite').save(dim_date_location_path)
    print(f'Registering date table with name {dim_date_table_name} in the metastore')
    register_delta_table(spark, dim_date_table_name, dim_date_location_path)
    print('Done...')

def transform_lookup_airport(dataframe):
    """Transform lookup airport dataframe and split the description column
    into airport_name, city and country

    Args:
        dataframe (dataframe): lookup airport spark dataframe 

    Returns:
        dataframe: lookup airport spark dataframe after being transformed
    """
    dataframe = (
        dataframe.select(
            F.col('Code').alias('airport_code'),
            F.split(F.split('Description', ', ')[1], ': ')[1].alias('airport_name'),
            F.split('Description', ', ')[0].alias('city'),
            F.split(F.split('Description', ', ')[1], ': ')[0].alias('country')
            )
    )
    return dataframe

def transform_lookup_plane(dataframe):
    """Transforms plane dataframe by dropping rows where all columns except tailnum is null

    Args:
        dataframe (dataframe): lookup plane spark dataframe 

    Returns:
        dataframe: lookup airport plane dataframe after being transformed
    """
    dataframe = (
        dataframe.dropna(how='all', subset=dataframe.columns[1:])
    )    

    return dataframe

