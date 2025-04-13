import logging
import os
from functools import reduce

from dags.landing.class_types import WeatherStationId
from dags.quality.quality_utils import (descriptive_profile,
                                        print_dataset_profile,
                                        compute_column_completeness,
                                        compute_relation_completeness,
                                        compute_attribute_timeliness,
                                        detect_TS_outliers,
                                        interpolate_missing,
                                        check_missing_dates)
from dags.utils.postgres_utils import PostgresManager
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
)
log = logging.getLogger(__name__)


def quality_weather(postgres_manager: PostgresManager):
    """Orchestrates quality processing for all weather stations."""
    for station in WeatherStationId:
        input_table = f"fmtted_weather_{station.name}"
        output_table = f"trusted_weather_{station.name}"
        quality_station_weather(input_table, output_table, postgres_manager)


def quality_station_weather(input_table: str, output_table: str, postgres_manager: PostgresManager) -> None:
    """Executes full Data Quality Pipeline for a weather station."""
    spark = SparkSession.builder \
        .config("spark.jars", os.getenv('JDBC_URL')) \
        .appName("WeatherQuality") \
        .getOrCreate()

    # 1. Read data
    df = postgres_manager.read_table(spark, input_table)
    log.info(f"Initial data count: {df.count()}")

    # 2. Data Profiling
    log.info("Generating Data Profiles")
    results = descriptive_profile(df)
    profile_print = print_dataset_profile(results)
    log.info(f"\nDataset profile for {input_table}:\n{'=' * 40}\n{profile_print}")

    # 3. Data Quality Metrics
    log.info("Computing Data Quality Metrics")

    # Completeness
    Q_cm_Att = compute_column_completeness(df)
    log.info("\nColumn completeness report:")
    for row in Q_cm_Att.collect():
        log.info(f"{row['column']}: {row['missing_ratio'] * 100:.2f}% missing")

    Q_cm_rel = compute_relation_completeness(df)
    log.info(f"Row completeness: {Q_cm_rel:.4f}")

    # Timeliness
    attribute_scores = 0
    for col in df.columns:
        if col != "datetime_iso":
            Q_t_Ai = compute_attribute_timeliness(df, col, "datetime_iso", 24).first()[1]
            attribute_scores += Q_t_Ai
            log.info(f"Timeliness for {col}: {Q_t_Ai:.4f}")

    # Corrected average timeliness calculation
    avg_timeliness = attribute_scores / (len(df.columns) - 1)
    log.info(f"Avg timeliness: {avg_timeliness:.4f}")

    # Redundancy
    Q_r = df.count() / df.dropDuplicates(["datetime_iso"]).count()
    log.info(f"Redundancy ratio: {Q_r:.4f}")

    # Missing dates check
    date_check = check_missing_dates(df, "datetime_iso")
    if not date_check['is_complete']:
        log.warning(f"Missing {len(date_check['missing_dates'])} dates")

    # 4. Core Weather Processing
    # Filter recent data and drop unstable columns
    recent_df = df.orderBy(F.desc("datetime_iso")).limit(730)
    unstable_cols = [col for col in recent_df.columns
                     if col not in ["datetime_iso", "station_id"]
                     and recent_df.filter(F.col(col).isNull()).count() > 730 * 0.2]
    filtered_df = df.drop(*unstable_cols)

    keep_columns = filtered_df.columns #Keep original columns

    # 5. Denial Constraints
    DC = {
        "non_negative_precipitation": F.col("PRCP") >= 0,
        "tmax_gte_tmin": F.col("TMAX") >= F.col("TMIN"),
        "valid_tmax_range": F.col("TMAX").between(-65, 60),
        "valid_tmin_range": F.col("TMIN").between(-65, 60),
    }

    # 6. Apply Constraints
    check_df = filtered_df.select('*')
    for name, constraint in DC.items():
        check_df = check_df.withColumn(f"check_{name}", constraint)

    all_checks = [F.col(f"check_{name}") for name in DC.keys()]
    check_df = check_df.withColumn("all_valid", reduce(lambda a, b: a & b, all_checks))

    Q_consistency = check_df.filter("all_valid").count() / df.count()
    log.info(f"Consistency ratio: {Q_consistency:.4f}")

    df = check_df.filter("all_valid")

    # 7. Data Cleaning

    # We don't handle outliers as the population distribution
    # is identical to the real population

    # Impute missing values
    final_df = interpolate_missing(df, "datetime_iso")

    # Remove duplicates
    final_df = final_df.dropDuplicates(["datetime_iso"])

    # 8. Post-Cleaning Metrics
    final_completeness = compute_relation_completeness(final_df)
    log.info(f"Final completeness: {final_completeness:.4f}")

    # 9. Keep desired columns
    final_df = final_df.select(*keep_columns)

    log.info("Schema before writing to Postgres:")
    final_df.printSchema()
    log.info("Sample data before writing:")
    final_df.show(5, truncate=False)
    if final_df.count() == 0:
        log.warning(f"DataFrame for {output_table} is empty. Skipping write.")

    # 10. Write Results
    postgres_manager.write_dataframe(final_df, output_table)
    spark.stop()
    log.info(f"Completed processing for {input_table}")
