from dags.utils.postgres_utils import PostgresManager
from dags.utils.other_utils import setup_logging
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import subprocess
import os

# Configure logging
log = setup_logging(__name__)

def format_electricity_data(postgres_manager: PostgresManager):
    """
    Formats electricity data loaded in HDFS w. Parquet format (LDWP and Pacific).
    """
    spark = None
    try:
        # Clean up temporary files from previous stage
        subprocess.run(["rm", "-rf", f'/tmp/electricity/'], check=True)

        # Initialize Spark session
        spark = SparkSession.builder \
            .config("spark.jars", os.getenv('JDBC_URL')) \
            .appName("ElectricityDataFormatter") \
            .getOrCreate()

        # 0. Read Data from HDFS
        # ======================
        base_hdfs_path = f"{os.getenv('HDFS_FS_URL')}/data/landing/electricity/"
        
        # 1. Create dataframe from all Parquet files
        # ==========================================
        df = spark.read.parquet(base_hdfs_path)
        
        # Inspect schema of the original data
        log.info("Original Data Schema:")
        df.printSchema()

        # 2. Variable Formatting 
        # ======================
        # Variable therminology renaming for consistency 
        df = df.withColumnRenamed("period", "date") \
            .withColumnRenamed("respondent", "respondent_id") \
            .withColumnRenamed("respondent-name", "respondent_name") \
            .withColumnRenamed("type", "data_type") \
            .withColumnRenamed("timezone", "timezone_id") \
            .withColumnRenamed("value-units", "unit_of_measure")

        # Data types conversion: 'date' column to DateType
        df = df.withColumn("date", F.to_date("date", "yyyy-MM-dd"))

        # Data types conversion: value to numerical
        df = df.withColumn("value", F.col("value").cast(DoubleType()))

        # Standarize to ISO 8601
        df = df.withColumn("datetime_iso", F.concat(
            F.col("date"), 
            F.lit(" 00:00")))
        df = df.withColumn("datetime_iso", F.to_timestamp("datetime_iso", "yyyy-MM-dd HH:mm")) #'datetime_iso' to timestamp

        df = df.withColumn("datetime_iso", F.date_format(
            F.col("datetime_iso"), "yyyy-MM-dd'T'HH:mm:ss'+00:00'"))
        
        
        # 3. Value Formatting
        # ===================
        # Categorical Value Mapping
        # -------------------------
        df = df.withColumn("unit_of_measure", F.when(F.col("unit_of_measure") == "megawatthours", "mwh") 
                                            .otherwise(F.col("unit_of_measure"))) # "megawatthours" -> "mwh"

        # Identify and convert all string columns to lowercase
        string_cols = ['respondent_id', 'respondent_name', 'data_type', 'timezone_id', 'unit_of_measure']
        for col in string_cols:
            df = df.withColumn(col, F.lower(F.col(col)))
        
        # # Trim all string columns and fix separate string values by replacing extra spaces with underscores
        for col in string_cols:
            df = df.withColumn(col, F.regexp_replace(F.trim(F.col(col)), r"\s+", "_"))

        # Homogenize NA indicators
        na_values = ['', 'NA', 'N/A', 'NULL']
        for col in df.columns:
            df = df.withColumn(col, F.when(F.col(col).isin(na_values), None).otherwise(F.col(col)))

        # Handle corrupted records
        if "_corrupt_record" in df.columns:
            corrupted_records = df.filter('_corrupt_record is not null')
            corrupted_records.show(truncate=False)

        # Show cleaned data sample and schema
        log.info("Final Schema:")
        df.printSchema()
        log.info("Sample Data:")
        df.show(5, truncate=False)

        # 4. Write to PostgreSQL
        # =============================
        postgres_manager.write_dataframe(df, table_name = "fmtted_electricity_data")
        log.info("Data written to PostgreSQL successfully.")

    except Exception as e:
        log.error(f"Error during data formatting: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
        log.info("Spark session closed.")