import logging
import os

from dags.utils.landing_utils.class_types import WeatherStationId # type:ignore
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def format_weather():
    """
    Loads data from all three selected stations
    """

    # Base hdfs path
    base_hdfs_path = f"{os.getenv('HDFS_FS_URL')}/data/landing/weather/"

    # Create hdfs landing path
    landing_long_beach = base_hdfs_path + WeatherStationId.LONG_BEACH.name
    landing_downtown = base_hdfs_path + WeatherStationId.DOWNTOWN.name
    landing_reseda = base_hdfs_path + WeatherStationId.RESEDA.name

    # Create POSTGRESQL table names
    long_beach_table = f"formatted_{WeatherStationId.LONG_BEACH.name}"
    downtown_table = f"formatted_{WeatherStationId.DOWNTOWN.name}"
    reseda_table = f"formatted_{WeatherStationId.RESEDA.name}"

    # Call functions
    format_station_weather(landing_long_beach, long_beach_table)
    format_station_weather(landing_downtown, downtown_table)
    format_station_weather(landing_reseda, reseda_table)


def format_station_weather(landing_path: str, table_name: str):
    # Initialize Spark session
    spark = SparkSession.builder \
        .config("spark.jars", os.getenv('JDBC_URL')) \
        .appName("ElementDataFormatter") \
        .getOrCreate()

    # Read Parquet files
    df = spark.read.parquet(landing_path)

    logging.info("Initial Schema:")
    df.printSchema()
    logging.info("\nInitial Sample Data:")
    df.show(5, truncate=False)

    # Convert DATE to standard date format (assuming 'yyyyMMdd' string)
    df = df.withColumn("DATE", F.to_date(F.col("DATE"), "yyyyMMdd"))

    # Pivot to create columns for each element
    pivoted_df = df.groupBy("DATE").pivot("ELEMENT").agg(F.first("DATA_VALUE"))

    pivoted_df = pivoted_df.orderBy("DATE", ascending=True)

    logging.info("Final Schema:")
    pivoted_df.printSchema()
    logging.info("\nSample Data:")
    pivoted_df.show(5, truncate=False)

    # Write to PostgreSQL (update connection details as needed)
    pivoted_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow") \
        .option("dbtable", table_name) \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    spark.stop()
