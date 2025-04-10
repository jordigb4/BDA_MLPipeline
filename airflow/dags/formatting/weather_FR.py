import logging
import os
import re

from dags.landing.class_types import WeatherStationId
from dags.utils.postgres_utils import PostgresManager
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, LongType


def format_weather(postgres_manager: PostgresManager):
    """
    Formats data from all three selected stations
    """

    # Base hdfs path
    base_hdfs_path = f"{os.getenv('HDFS_FS_URL')}/data/landing/weather/"

    for station in [WeatherStationId.LONG_BEACH, WeatherStationId.DOWNTOWN, WeatherStationId.RESEDA]:
        landing_path = base_hdfs_path + station.name
        table_name = f"formatted_weather_{station.name}"
        format_station_weather(landing_path, table_name, postgres_manager)


def format_station_weather(landing_path: str, table_name: str, postgres_manager: PostgresManager):
    # Initialize Spark session
    spark = SparkSession.builder \
        .config("spark.jars", os.getenv('JDBC_URL')) \
        .appName("ElementDataFormatter") \
        .getOrCreate()

    # 1. Create dataframe from RDD (in hdfs) and schema
    # =======================================
    schema = StructType([
        StructField("STATION", StringType(), True),
        StructField("DATE", StringType(), True),
        StructField("OBS_TIME", StringType(), True),
        StructField("ELEMENT", StringType(), True),
        StructField("DATA_VALUE", LongType(), True),
        StructField("parsed_date", DateType(), True)
    ])

    df = spark.read.schema(schema).parquet(landing_path)

    logging.info("Initial Schema:")
    df.printSchema()
    logging.info("\nInitial Sample Data:")
    df.show(5, truncate=False)

    # 2. Variable Formatting
    # ======================

    # Variable names already correct

    # 3. Value Formatting
    # ===================

    # Date formats: standarize to ISO 8601
    # Time Zone Alignement: UTC -> already in the data!
    df = df.withColumn("datetime_iso", F.to_date(F.col("DATE"), "yyyyMMdd"))
    # Homogenize null indicators
    poss_nulls = ["NA", "N/A", "null", "", " "]
    df = df.na.replace(poss_nulls, None)

    # Check non-null constraints
    df = df.filter(
        F.col("datetime_iso").isNotNull() &
        F.col("DATA_VALUE").isNotNull()
    )

    # 4. Format DataFrame Structure
    # =============================

    pivoted_df = df.groupBy("datetime_iso").pivot("ELEMENT").agg(F.first("DATA_VALUE"))

    pivoted_df = pivoted_df.orderBy("datetime_iso", ascending=True)

    logging.info("Final Schema:")
    pivoted_df.printSchema()
    logging.info("\nSample Data:")
    pivoted_df.show(5, truncate=False)

    # 5. Write to PostgreSQL
    # =============================
    postgres_manager.write_dataframe(pivoted_df, table_name)

    spark.stop()
