from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Minimum logging level
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
)

log = logging.getLogger(__name__)

def format_electricity_data_parquet(hdfs_manager):
    """
    Formats electricity data loaded from Parquet (LDWP and Pacific).
    """
    try:
        # Create a Spark session
        spark = SparkSession.builder \
            .appName("ElectricityDataFormatter") \
            .getOrCreate()

        # Define the path for the Parquet file (adjust according to your environment)
        parquet_path = "/data/landing/electricity/2019-01-01_2024-03-31.parquet"  # Replace with the actual path
        
        # Load Parquet data
        df = spark.read.parquet(parquet_path)
        
        # Inspect schema of the original data
        log.info("Original Data Schema:")
        df.printSchema()

        # Rename columns for consistency
        df = df.withColumnRenamed("period", "date") \
            .withColumnRenamed("respondent", "respondent_id") \
            .withColumnRenamed("respondent-name", "respondent_name") \
            .withColumnRenamed("type", "data_type") \
            .withColumnRenamed("timezone", "timezone_id") \
            .withColumnRenamed("value-units", "unit_of_measure")

        # Convert 'date' column to DateType
        df = df.withColumn("date", F.to_date("date", "yyyy-MM-dd"))

        # Create datetime_iso column
        df = df.withColumn("datetime_iso", F.concat(
            F.col("date"), 
            F.lit(" 00:00")))

        # Convert string to timestamp
        df = df.withColumn("datetime_iso", F.to_timestamp("datetime_iso", "yyyy-MM-dd HH:mm"))

        # Format timestamp to ISO 8601
        df = df.withColumn("datetime_iso", F.date_format(
            F.col("datetime_iso"), "yyyy-MM-dd'T'HH:mm:ss'+00:00'"))
        
        # Convert 'value' column to DoubleType
        df = df.withColumn("value", F.col("value").cast(DoubleType()))

        # Replace "megawatthours" with "mwh"
        df = df.withColumn("unit_of_measure", F.when(F.col("unit_of_measure") == "megawatthours", "mwh")
                                            .otherwise(F.col("unit_of_measure")))

        # Identify string columns
        string_cols = ['respondent_id', 'respondent_name', 'data_type', 'timezone_id', 'unit_of_measure']

        # Convert all string columns to lowercase
        for col in string_cols:
            df = df.withColumn(col, F.lower(F.col(col)))
        
        # Clean up string columns by replacing extra spaces with underscores
        for col in string_cols:
            df = df.withColumn(col, F.regexp_replace(F.trim(F.col(col)), r"\s+", "_"))

        # Handle missing values by replacing known invalid entries with null
        na_values = ['', 'NA', 'N/A', 'NULL']
        for col in df.columns:
            df = df.withColumn(col, F.when(F.col(col).isin(na_values), None).otherwise(F.col(col)))

        # Handle corrupted records
        if "_corrupt_record" in df.columns:
            corrupted_records = df.filter('_corrupt_record is not null')
            corrupted_records.show(truncate=False)

        



        # Show some cleaned data for inspection
        log.info("Cleaned Data Sample:")
        df.show(5, truncate=False)

        # Save the cleaned data to Parquet file
        output_file = "/data/landing/electricity/formatted_data.parquet"
        df.write.parquet(output_file, mode="overwrite")
        log.info(f"Formatted data saved to {output_file}")

        # 7. HDFS Storage
        log.info("Transferring to HDFS...")
        hdfs_manager.copy_from_local(output_file, "/data/landing/electricity")

    except Exception as e:
        log.error(f"Error during data formatting: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
        log.info("Spark session closed.")
