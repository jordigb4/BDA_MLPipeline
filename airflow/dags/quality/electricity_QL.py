import logging
import os

from dags.quality.quality_utils import *
from dags.utils.postgres_utils import PostgresManager
from pyspark.sql import SparkSession

# Configura el logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
)

log = logging.getLogger(__name__)


def quality_electricity(input_table: str, output_table: str, postgres_manager: PostgresManager) -> None:
    """
    Executes the whole Data Quality Pipeline for one particular air station.
    """

    # Initialize Spark session
    spark = SparkSession.builder \
        .config("spark.jars", os.getenv('JDBC_URL')) \
        .appName("ElectricityDataQuality") \
        .getOrCreate()

    # Set the legacy time parser policy to avoid the date parsing error
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    # 1. Read the table from PostgreSQL
    # =================================
    df = postgres_manager.read_table(spark, input_table)
    log.info(f"Initial data count: {df.count()}")

    # 2. Generate Data Profiles (descriptive statistics)
    # =========================
    log.info("Generating Data Profiles")

    results = descriptive_profile(df)
    profile_print = print_dataset_profile(results)
    print(f"\nDataset profile for {input_table}:\n{'=' * 40}\n{profile_print}")

    # 3. Computation of Data Quality Metrics
    # =======================================
    log.info("Computing Data Quality Metrics")

    # Completeness: non-missing ratios per attribute
    Q_cm_Att = compute_column_completeness(df)
    print("\nColumn completeness report:")
    print("-" * 36);
    print(f"{'Column':<25} | {'Missing (%)':>10}");
    print("-" * 36)

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
            Q_t_Ai = compute_attribute_timeliness(df, col, transactionTime_col="datetime_iso", update_rate=12).first()[
                1]
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

    # Check if there are missing dates
    missing_dates_result = check_missing_dates(df)

    # Display results for missing dates
    if missing_dates_result['is_complete']:
        print("\nAll dates are present in the dataset.")
    else:
        print(f"\nMissing dates: {', '.join(str(date) for date in missing_dates_result['missing_dates'])}")
        print(f"Total dates in the range: {len(missing_dates_result['all_dates'])}")
        print(
            f"Dates present in the dataset: {len(missing_dates_result['all_dates']) - len(missing_dates_result['missing_dates'])}")

    # 4. Denial Constraints Definition 
    # =================================

    DC = {
        "pos_value": F.col("value") >= 0,  # Ensure that the value is positive
        "valid_date": F.to_date(F.col("date"), "yyyy-MM-dd").isNotNull(),
        "value_realistic": F.col("value") <= 500000,
        "valid_unit_of_measure": F.col("unit_of_measure").isin("mwh"),  # Only one unit of measure possible
        "valid_timezone": F.col("timezone_id").isin("pacific", "eastern", "central"),  # Valid timezone
    }

    # 4. Apply the denial constraints
    # ================================
    # Apply the constraints one by one
    for constraint_name, constraint_expr in DC.items():
        log.info(f"Applying constraint: {constraint_name}")
        df = df.filter(constraint_expr)  # Keep only rows that meet the condition

    # 6. Data Cleaning

    # Remove duplicates
    cleaned_df = df.dropDuplicates(["datetime_iso"])

    #Remove low variance columns
    low_cardinality_cols = [f"{key}={cleaned_df.select(key).first()[0]}" for key, count in results["unique_counts"].items() if count == 1]
    log.info(f"Separate columns that are metadata: {', '.join(low_cardinality_cols)}")

    # Impute missing values
    final_df = interpolate_missing(cleaned_df, "datetime_iso")

    # 7. Post-Cleaning Metrics
    final_completeness = compute_relation_completeness(final_df)
    log.info(f"Final completeness: {final_completeness:.4f}")

    log.info("Schema before writing to Postgres:")
    final_df.printSchema()
    log.info("Sample data before writing:")
    final_df.show(5, truncate=False)
    if final_df.count() == 0:
        log.warning(f"DataFrame for {output_table} is empty. Skipping write.")

    # 8. Write Results
    postgres_manager.write_dataframe(final_df, output_table)
    spark.stop()
    log.info(f"Completed processing for {input_table}")

    if spark:
        spark.stop()
        log.info("Spark session closed")
