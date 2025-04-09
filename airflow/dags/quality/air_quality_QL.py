
import os
from dags.utils.other_utils import setup_logging
from dags.landing.class_types import AirStationId
from dags.utils.postgres_utils import PostgresManager
from dags.quality.quality_utils import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Create a module-specific logger
log = setup_logging(__name__)

def quality_air(postgres_manager: PostgresManager):
    """
    Improves and analises quality of all three stations data.
    """
    for station in [AirStationId.LONG_BEACH, AirStationId.DOWNTOWN, AirStationId.RESEDA]:

        # Input Table
        inTable_name = f"fmtted_airQuality_{station.name}"
        # Output Table
        outTable_name = f"trusted_airQuality_{station.name}"

        # Call Data Quality Pipeline
        quality_station_air(inTable_name, outTable_name, postgres_manager)

    
def quality_station_air(input_table: str, output_table: str, postgres_manager: PostgresManager) -> None:
    """
    Executes the whole Data Quality Pipeline for one particular air station.
    """

    # Initialize Spark session
    spark = SparkSession.builder \
        .config("spark.jars", os.getenv('JDBC_URL')) \
        .appName("AirQuality") \
        .getOrCreate()

    # 1. Read the table from PostgreSQL
    # =================================
    df = postgres_manager.read_table(spark, input_table)

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
    Q_cm_Att = compute_column_completeness(df)
    print("\nColumn completeness report:")
    print("-" * 36); print(f"{'Column':<25} | {'Missing (%)':>10}"); print("-" * 36)

    for row in Q_cm_Att.collect():
        missing_pct = f"{row['missing_ratio'] * 100:.2f}%"
        print(f"{row['column']:<25} | {missing_pct:>10}")
        print("-" * 36 + "\n")
    
    # Completeness: for the whole dataset (rows with no missing values)
    Q_cm_rel = compute_relation_completeness(df)
    print(f"\nRelation's Completeness (ratio of complete rows): {Q_cm_rel:.4f}")

    # Timeliness: check if the data is up to date
    attribute_scores = 0
    for col in df.columns:
        if col != "datetime_iso":
            # Compute timeliness for each attribute
            Q_t_Ai = compute_attribute_timeliness(df, col, transactionTime_col="datetime_iso", update_rate = 12).first()[1]
            attribute_scores += Q_t_Ai
            print(f"Timeliness score for {col}: {Q_t_Ai:.4f}")
    Q_t_R = attribute_scores / (len(df.columns) - 1)
    print(f"\nRelation's Timeliness (average of attributes): {Q_t_R:.4f}")

    # Redundancy: check for duplicate rows
    Q_r = df.count() / df.dropDuplicates(["datetime_iso"]).count()
    print(f"\nRelation's Redundancy (ratio of duplicates): {Q_r:.4f}")

    # Redundancy: check for attributes with only one level
    one_value_att = [att for att, value in results['unique_counts'].items() if value == 1]
    if one_value_att:
        print(f"\nAttributes with only one value: {', '.join(one_value_att)}")
    else:
        print("\nNo attributes with only one value found.")

    # Accuracy and consistency will be checked in next steps, as they require domain knowledge.

    # 4. Denial Constraints Definition 
    #=================================
    parameter_columns = set(results["numeric_columns"]) - set(["lat", "lon"])
    DC = {
        # All air quality parameters must be positive
        "pos_params": f"({', '.join(parameter_columns)}) >= 0",
        
        "valid_lat": (F.col("lat") >= -90) & (F.col("lat") <= 90), # Latitude range (-90 to 90)
        "valid_lon": (F.col("lon") >= -180) & (F.col("lon") <= 180), # Longitude range (-180 to 180)

        "ordered_timestamps": F.col("datetime_iso") > F.lag("datetime_iso").over(
            Window.partitionBy("location").orderBy("datetime_iso")), # Chronological timestamps

        "PM_cons": F.col("pm10") >= F.col("pm25"), # physical consistency

        # PM constraints (µg/m³): expert knowledge based
        "pm25_realistic": F.col("pm25") < 250,  # Extreme pollution threshold
        "pm10_realistic": F.col("pm10") < 500,  # Extreme pollution threshold

        # Gaseous pollutants (ppm)
        "co_realistic": F.col("co") < 50,       # CO - Extreme: 50ppm
        "no_realistic": F.col("no") < 10,       # NO - Extreme industrial areas
        "no2_realistic": F.col("no2") < 1,      # NO₂ - 1ppm
        "nox_realistic": F.col("nox") < 2,      # NOx (NO + NO₂ combined)
        "o3_realistic": F.col("o3") < 0.2,      # O₃ - Extreme smog: 0.2ppm

        # NOx = NO + NO2 (when all components are present)
        "check_NOx": (
            (F.col("no").isNull()  | 
             F.col("no2").isNull() | 
             F.col("nox").isNull())| 
            (F.abs(F.col("nox") - (F.col("no") + F.col("no2"))) < 0.001)),

        # Mandatory one parameter value per timestamp
        "non_null_row": F.reduce(lambda a, b: a | b,
                        [F.col(c).isNotNull() for c in parameter_columns])
    }
    print(DC)
    # 4. Apply the denial constraints
    #================================
    log.info("Applying Denial Constraints")

    if spark:
        spark.stop()
        log.info("Spark session closed")
