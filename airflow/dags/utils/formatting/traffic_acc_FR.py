from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from dags.utils.landing.class_types import TrafficAccId
from dags.utils.postgres_utils import PostgresManager
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import subprocess
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
)

log = logging.getLogger(__name__)


def format_traffic_acc(postgres_manager: PostgresManager):
    """
    Formats data from all three selected areas
    """

    # Clean up temporary files from previous stage
    subprocess.run(["rm", "-rf", f'/tmp/traffic_acc/'], check=True)

    base_hdfs_path = f"{os.getenv('HDFS_FS_URL')}/data/landing/traffic_acc/"

    for area in [TrafficAccId.LONG_BEACH, TrafficAccId.DOWNTOWN, TrafficAccId.RESEDA]:
        landing_path = base_hdfs_path + area.name
        table_name = f"fmtted_trafficAcc_{area.name}"
        format_area_acc(landing_path, table_name, postgres_manager)


def format_area_acc(landing_path: str, table_name: str, postgres_manager):
    """Main ETL function to format traffic accident data."""
    
    spark = None
    try:
        spark = SparkSession.builder \
            .config("spark.jars", os.getenv('JDBC_URL')) \
            .appName("TrafficAccFormatter") \
            .getOrCreate()

        # 0. Read Data from HDFS (read JSON files)
        # ======================

        # Define reading schema
        schema = StructType([
            StructField("dr_no", StringType(), nullable=False),
            StructField("date_rptd", TimestampType(), nullable=True),
            StructField("date_occ", TimestampType(), nullable=False),
            StructField("time_occ", StringType(), nullable=True),
            StructField("area", StringType(), nullable=True),
            StructField("area_name", StringType(), nullable=True),
            StructField("rpt_dist_no", StringType(), nullable=True),
            StructField("crm_cd", StringType(), nullable=True),
            StructField("crm_cd_desc", StringType(), nullable=True),
            StructField("mocodes", StringType(), nullable=True),
            StructField("vict_age", StringType(), nullable=True),
            StructField("vict_sex", StringType(), nullable=True),
            StructField("vict_descent", StringType(), nullable=True),
            StructField("premis_cd", StringType(), nullable=True),
            StructField("premis_desc", StringType(), nullable=True),
            StructField("location", StringType(), nullable=True),
            StructField("cross_street", StringType(), nullable=True),
            StructField("location_1", StructType([
                StructField("latitude", StringType(), nullable=True),
                StructField("longitude", StringType(), nullable=True),
                StructField("human_address", StringType(), nullable=True)
            ]), nullable=True)
        ])

        # 1. Create dataframe from JSON and schema
        # ========================================
        df = spark.read \
            .schema(schema) \
            .option("multiLine", True) \
            .option("mode", "PERMISSIVE") \
            .json(f"{landing_path}/*.json")
        # Obs: "multiline" to read records that can span multiple lines. UTF-8 encoding is default.

        # 2. Variable Formatting 
        # ======================
        # Get longitude and latitude from location_1 nested structure
        df = (df.select("*", F.col("location_1.latitude").alias("lat_str"), F.col("location_1.longitude").alias("lon_str"))
              .drop("location_1") # human_addr has no information
        )

        # Data types conversion: to numerical
        df = (df
              .withColumn("latitude", F.col("lat_str").cast(DoubleType()))
              .withColumn("longitude", F.col("lon_str").cast(DoubleType()))
              .withColumn("vict_age", F.col("vict_age").cast("integer"))
              .drop("lat_str", "lon_str")
             )
        
        # Data types conversion: to array
        df = df.withColumn("mocodes",
            F.when(F.col("mocodes").isNotNull(),
                F.split(F.regexp_replace(F.col("mocodes"), r"\s+", ","), ",")).otherwise(F.array())
        )

        # Fix time_occ format
        df = df.withColumn("time_occ", F.concat(
                                F.substring("time_occ", 1, 2),
                                F.lit(":"),
                                F.substring("time_occ", 3, 2))
        )
        # Result: HHmm -> HH:mm
        
        # Date formats: standarize to ISO 8601 by merging date_occ and time_occ
        # Time Xone Alignement: UTC -> already in the data!
        df = df.withColumn("datetime_iso", F.date_format(
                            F.to_timestamp(
                                F.concat(
                                    F.substring(F.col("date_occ").cast("string"), 1, 10),  # Extract just the date part
                                    F.lit(" "),
                                    F.col("time_occ")),
                                    "yyyy-MM-dd HH:mm"),
                                "yyyy-MM-dd'T'HH:mm:ss'+00:00'")
        )
        df = df.withColumn("datetime_iso", F.col("datetime_iso").cast(TimestampType()))
        
        # Result:
        # 2019-04-11 00:00:00 (date) & 05:40 (time) -> 2019-04-11T05:40:00XXX

        # 3. Value Formatting
        # ===================
        # Trim all string columns and fix separate string values (e.g. SATICOY                      ST) -> consistency!
        string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
        for col in string_cols:
            df = df.withColumn(col, F.regexp_replace(F.trim(F.col(col)),  # Remove extra spaces
                                         r"\s+", "_"  # Replace inner spaces with underscores
                                    ))

        # Homogenize NA indicators
        na_values = ['', 'NA', 'N/A', 'NaN', 'NULL']
        for col in string_cols:
            df = df.withColumn(col, F.when(F.col(col).isin(na_values), None).otherwise(F.col(col)))

        # Categorical Value Mapping
        # -------------------------
        # "X" values in victim columns mean "Unknown" code
        for col_name in ['vict_sex', 'vict_descent']:
            df = df.withColumn(col_name,
                F.when(F.col(col_name) == 'X', 'Unknown')
                .otherwise(F.col(col_name))
            )

        # Expand victim descent codes to full names
        vict_descent_map = {'A': 'Other Asian', 'B': 'Black', 'C': 'Chinese', 'D': 'Cambodian', 'F': 'Filipino', 'G': 'Guamanian', 
                            'H': 'Hispanic', 'I': 'Native American', 'J': 'Japanese', 'K': 'Korean', 'L': 'Laotian', 'O': 'Other', 
                            'P': 'Pac. Islander','S': 'Samoan', 'U': 'Hawaiian', 'V': 'Vietnamese', 'W': 'White', 'X': 'Unknown', 
                            'Z': 'Asian Indian'}

        # Convert dictionary to a Spark map expression
        mapping_expr = F.create_map([F.lit(x) for x in sum(vict_descent_map.items(), ())])
        # Apply the mapping
        df = df.withColumn('vict_descent', mapping_expr.getItem(F.col('vict_descent')))

        # Check corrupted records
        if "_corrupt_record" in df.columns:
            log.warning(f"Found {df.filter('_corrupt_record is not null').count()} corrupt records. Check for details.")
        
        log.info("Final Schema:")
        df.printSchema()
        log.info("Sample Data:")
        df.show(5, truncate=False)

        # 4. Write to PostgreSQL
        # =============================
        postgres_manager.write_dataframe(df, table_name)

    except Exception as e:
        log.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
        log.info("Spark session closed.")