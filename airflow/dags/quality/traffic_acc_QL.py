from dags.utils.postgres_utils import PostgresManager
from dags.landing.class_types import TrafficAccId
from dags.utils.other_utils import setup_logging
from dags.quality.quality_utils import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from functools import reduce
import os

# Create a module-specific logger
log = setup_logging(__name__)


def quality_traffic(postgres_manager: PostgresManager):
    """
    Improves and analises quality of all three stations data.
    """
    for area in [TrafficAccId.LONG_BEACH, TrafficAccId.DOWNTOWN, TrafficAccId.RESEDA]:

        # Input Table
        inTable_name = f"fmtted_trafficAcc_{area.name}"
        # Output Table
        outTable_name = f"trusted_trafficAcc_{area.name}"

        # Call Data Quality Pipeline
        quality_station_traffic(inTable_name, outTable_name, postgres_manager)


def quality_station_traffic(input_table: str, output_table: str, postgres_manager: PostgresManager) -> None:
    """
    Executes the whole Data Quality Pipeline for one particular air station.
    """

    # Initialize Spark session
    spark = SparkSession.builder \
        .config("spark.jars", os.getenv('JDBC_URL')) \
        .appName("TrafficQuality") \
        .getOrCreate()

    # 1. Read the table from PostgreSQL
    # =================================
    df = postgres_manager.read_table(spark, input_table)
    df.show(5)

    # 2. Generate Data Profiles (descriptive statistics)
    # =========================
    log.info("Generating Data Profiles")

    results = descriptive_profile(df)
    profile_print = print_dataset_profile(results)
    print(f"\nDataset profile for {input_table}:\n{'=' * 40}\n{profile_print}")

    # 3. Computation of Data Quality Metrics
    #=======================================
    log.info("Computing Data Quality Metrics")
    
    # Completeness: non-missing ratios per attribute
    Q_cm_Att = compute_column_completeness(df); output_lines = list()
    output_lines.append("\nColumn completeness report:")
    output_lines.append(f" {'-' * 36} \n{'Column':<25} | {'Missing (%)':>10} \n{'-' * 36}")

    for row in Q_cm_Att.collect():
        missing_pct = f"{row['missing_ratio'] * 100:.2f}%"
        output_lines.append(f"{row['column']:<25} | {missing_pct:>10} \n{'-' * 36}")
    
    # Completeness: for the whole dataset (rows with no missing values)
    Q_cm_rel = compute_relation_completeness(df)
    print(f"\nRelation's Completeness (ratio of complete rows): {Q_cm_rel:.4f}")
    print("\n".join(output_lines))



    # Sample of final data
    print("\nSample of final data:")
    print("\n".join(output_lines))
    print(f"Enhanced data has {df.count()} rows and {len(df.columns)} columns.")

    # Export df with enhanced quality to PostgreSQL
    # =============================================
    log.info("Exporting DataFrame to PostgreSQL")
    postgres_manager.write_dataframe(df, output_table)
    
    if spark:
        spark.stop()
        log.info("Spark session closed")