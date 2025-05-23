from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from dags.utils.postgres_utils import PostgresManager
from dags.landing.class_types import AirStationId
from dags.utils.other_utils import setup_logging
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from io import BytesIO
import subprocess
import tarfile
import gzip
import csv
import re
import os

# Configure logging
log = setup_logging(__name__)


def format_air_quality(postgres_manager: PostgresManager):
    """
    Formats data from all three selected stations
    """

    # Clean up temporary files from previous stage
    subprocess.run(["rm", "-rf", f'/tmp/air_quality/'], check=True)

    base_hdfs_path = f"{os.getenv('HDFS_FS_URL')}/data/landing/air_quality/"

    for station in [AirStationId.LONG_BEACH, AirStationId.DOWNTOWN, AirStationId.RESEDA]:
        landing_path = base_hdfs_path + station.name
        table_name = f"fmtted_airQuality_{station.name}"
        format_station_air(landing_path, table_name, postgres_manager)


def format_station_air(landing_path: str, table_name: str, postgres_manager: PostgresManager):
    """Main ETL function to format air quality data"""

    spark = None
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .config("spark.jars", os.getenv('JDBC_URL')) \
            .config("spark.sql.csv.parser.columnPruning.enabled", "true") \
            .appName("AirQualityFormatter") \
            .getOrCreate()

        # 0. Read Data from HDFS (read tar.gz files as binary streams)
        # ======================
        tar_gz_rdd = spark.sparkContext.binaryFiles(landing_path + "/*.tar.gz")

        # Process files in parallel and create DataFrame
        def process_tar_file(file_content):
            """
            Recieves a binary file content from Spark's binaryFiles RDD
            Exploit lazy Evaluation from Spark's execution model!
            """
            try:
                # Create an in-memory buffer -> avoid writing to disk
                buffer = BytesIO(file_content)
                with tarfile.open(fileobj=buffer, mode="r:gz") as tar:
                    # iterates through the tar's files, which are all gzipped CSV
                    for member in tar.getmembers():
                        with tar.extractfile(member) as f:
                            # Uniform text encoding -> utf-8
                            with gzip.open(f, 'rt', encoding="utf-8") as gz_file:
                                # Decompress gzip and map variable: value in a dict
                                reader = csv.DictReader(gz_file, delimiter=",", escapechar="\\")
                                for row in reader:
                                    # Remove leading and trailing whitespaces -> consistency
                                    yield {k: v.strip() for k, v in row.items()}
            except Exception as e:
                log.warning(f"Error processing file: {e}")
                return iter([])
        
        # 1. Create dataframe from RDD and schema
        # =======================================
        rows_rdd = tar_gz_rdd.flatMap(lambda x: process_tar_file(x[1]))
        log.info(rows_rdd.take(5))

        # Define reading schema
        schema = StructType([
                    StructField("location_id", StringType(), nullable=False),
                    StructField("sensors_id", StringType(), nullable=True),
                    StructField("location", StringType(), nullable=True),
                    StructField("datetime", StringType(), nullable=False),
                    StructField("lat", StringType(), nullable=True),
                    StructField("lon", StringType(), nullable=True),
                    StructField("parameter", StringType(), nullable=True),
                    StructField("units", StringType(), nullable=True),
                    StructField("value", StringType(), nullable=False)
                ])
        
        df = spark.createDataFrame(rows_rdd, schema=schema)

        # 2. Variable Formatting 
        # ======================
        def camel_to_snake(name):
            # Insert underscores before capital letters, then lowercase
            return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
        
        # Variable therminology formatting ->  convert to snake_case
        df = df.toDF(*[camel_to_snake(c) for c in df.columns])

        # Data types conversion: to numerical
        df = (df
            .withColumn("lat", F.col("lat").cast(DoubleType()))
            .withColumn("lon", F.col("lon").cast(DoubleType()))
            .withColumn("value", F.col("value").cast(DoubleType()))
            )

        # 3. Value Formatting
        # ===================

        # Date formats: standarize to ISO 8601
        # Time Xone Alignement: UTC -> already in the data!
        df = df.withColumn("datetime_iso", F.to_timestamp("datetime", "yyyy-MM-dd'T'HH:mm:ssXXX"))
        df = df.drop("datetime")
        # Result:
        # 04/04/2025 9:00:00 -> 2025-04-04T09:00:00XXX

        # Homogenize null indicators
        poss_nulls = ["NA", "N/A", "null", "", " "]
        df = df.na.replace(poss_nulls, None)

        # Check non-null constraints
        df = df.filter(
            F.col("datetime_iso").isNotNull() &
            F.col("parameter").isNotNull() &
            F.col("value").isNotNull()
        )
        
        # 4. Format DataFrame Structure
        # =============================

        # Pivot to add a column for each parameter
        pivoted_df = df.groupBy(["datetime_iso", "location", "lat", "lon"]).pivot("parameter").agg(F.first("value"))
        # Order instances chronologically
        pivoted_df = pivoted_df.orderBy("datetime_iso", ascending=True)

        log.info("Final Schema:")
        pivoted_df.printSchema()
        log.info("\nSample Data:")
        pivoted_df.show(5, truncate=False)

        # 5. Write to PostgreSQL
        # =============================
        postgres_manager.write_dataframe(pivoted_df, table_name)
        
    except Exception as e:
        log.error(f"Fatal pipeline error: {str(e)}", exc_info=True)
        raise

    finally:
        if spark:
            spark.stop()
        log.info("Spark session closed")