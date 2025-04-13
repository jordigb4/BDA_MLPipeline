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
    print(df.count(), df.dropDuplicates(["datetime_iso"]).count())
    print(f"\nRelation's Redundancy (ratio of duplicates): {Q_r:.4f}")

    # Redundancy: check for attributes with only one level
    one_value_att = [att for att, value in results['unique_counts'].items() if value == 1]
    if one_value_att:
        print(f"\nAttributes with only one value: {', '.join(one_value_att)}")
    else:
        print("\nNo attributes with only one value found.")

    # Accuracy and consistency will be checked in next steps, as they require domain knowledge.

    # 4. Denial Constraints Definition: using metadata from LAPD
    #=================================
    DC = {}

    # 1. Temporal Consistency: reported date should not precede occurrence date
    DC["valid_report_date"] = F.col("date_rptd") >= F.col("date_occ")

    # 2. Valid time format (HH:MM)
    DC["valid_time_format"] = F.col("time_occ").rlike("^[0-2][0-9]:[0-5][0-9]$")

    # 3. Geographic Constraints
    DC["valid_latitude"] = (F.col("latitude") >= 33.0) & (F.col("latitude") <= 34.5)  # LA area range
    DC["valid_longitude"] = (F.col("longitude") >= -119.0) & (F.col("longitude") <= -117.5)

    # 4. Victim Data Quality: age constraint (0-120 years possible)
    DC["valid_vict_age"] = (F.col("vict_age").between(0, 120))

    # 5. Valid sex codes (M/F/X/Unknwon)
    DC["valid_vict_sex"] = F.col("vict_sex").isin(["M", "F", "X", "Unknown", None])

    # 6. Crime Data Integrity: valid area codes (LAPD divisions 1-21)
    DC["valid_area"] = F.col("area").between(1, 21)

    # 7. Crime code validity (100-999 per LAPD coding system)
    DC["valid_crm_cd"] = F.col("crm_cd").between(100, 999)

    # 8. Premise Validation: valid codes (101=Street, 108=Parking Lot, etc.)
    valid_premis = [101, 102, 103, 104, 105, 106, 107, 108]
    DC["valid_premis"] = F.col("premis_cd").isin(valid_premis)

    # 9. Street incidents must have location and cross street
    DC["street_location"] = ((F.col("premis_desc") != "STREET") | 
                            ((F.col("location").isNotNull()) & (F.col("cross_street").isNotNull())))
    
    # 10. Identifier Constraints: unique and non-negative
    DC["valid_dr_no"] = F.col("dr_no") > 0

    # Visualize the dict of constraints
    print("\n" + "\n".join(
    ["Denial Constraints:", '-' * 40] +
    [f"{name}: {constraint}" for name, constraint in DC.items()]))

    # 5. Apply the denial constraints
    #================================
    log.info("Applying Denial Constraints")

    # Create a new dataframe for constraint checking
    check_df = df.select('*')
    n_rows = check_df.count()

    # Add constraint check columns to the new dataframe
    for constraint_name, constraint in DC.items():
        check_df = check_df.withColumn(f"check_{constraint_name}", constraint)

    # Combine all constraints into a single "all_valid" column
    all_rules_check = [f"check_{name}" for name in DC.keys()]
    check_df = check_df.withColumn("all_rules_check", F.expr(" AND ".join(all_rules_check)))

    # Consistency: proportion of rows that satisfy all constraints
    Q_cn = check_df.filter(F.col("all_rules_check")).count() / n_rows
    print(f"\nRelation's Consistency: {Q_cn:.4f}")

    # Compute per-constraint statistics
    constraint_stats = {}; print('-' * 40)
    for constraint_name in DC.keys():
        constraint_count = check_df.filter(F.col(f"check_{constraint_name}")).count()
        constraint_stats[constraint_name] = constraint_count / n_rows
        print(f"Constraint '{constraint_name}' satisfied: {constraint_stats[constraint_name]:.4f}")
    print('-' * 40)

    # 6. Outlier Detection
    #====================
    # There aren't outliers since data is categorical

    # 7. Improve the quality of the data
    #===================================

    # Constraint Enforcing:
    # ---------------------
    log.info("Enforcing Denial Constraints")

    # Map constraints to their affected columns
    constraint_mapping = {"valid_report_date": ["date_rptd", "date_occ"], "valid_time_format": ["time_occ"],
                          "valid_latitude": ["latitude"], "valid_longitude": ["longitude"], "valid_vict_age": ["vict_age"],
                          "valid_vict_sex": ["vict_sex"], "valid_area": ["area"], "valid_crm_cd": ["crm_cd"],
                          "valid_premis": ["premis_cd"], "street_location": ["location", "cross_street"], "valid_dr_no": ["dr_no"]}

    # Apply constraints in sequence
    for constraint_name, columns in constraint_mapping.items():
        if constraint_name in DC:
            condition = DC[constraint_name]
            
            # Special handling for street locations
            if constraint_name == "street_location":
                df = df.withColumn("location", F.when(condition, F.col("location")).otherwise(F.lit(None))) \
                    .withColumn("cross_street", F.when(condition, F.col("cross_street")).otherwise(F.lit(None)))
            else:
                for col in columns:
                    # Convert to None if the condition is not met -> then impute
                    df = df.withColumn(col, F.when(condition, F.col(col)).otherwise(F.lit(None)))

    # Special case: Clean victim sex values
    df = df.withColumn("vict_sex", F.when(F.col("vict_sex").isin(["M", "F", "X"]), F.col("vict_sex"))
                                    .otherwise(F.lit("Unknown")))

    # # Remove tuples:
    # # --------------

    # Duplicates
    df = df.dropDuplicates(["dr_no"]); n_rows_new = df.count()
    log.info(f"Duplicates removed: {n_rows - n_rows_new}")

    # Tuples with lots of missing values (i.e. more than 70% of parameters)
    null_count = reduce(lambda a, b: a + b, [F.isnull(col).cast("int") for col in df.columns])
    df = df.filter((null_count / len(df.columns)) < 0.7)
    log.info(f"Rows with too many missing values removed: {n_rows_new - df.count()}")
    
    # Remove attributes:    
    # ------------------
    # Low variance columns: this is metadata, separate it from data!
    unique_cols = [key for key, count in results["unique_counts"].items() if count == 1]
    low_cardinality_cols = [f"{key}={df.select(key).first()[0]}" for key in unique_cols]
    df = df.drop(*unique_cols)
    log.info(f"Separate columns that are metadata: {', '.join(low_cardinality_cols)}")

    # Remove columns with high missing values (i.e. > 50 %), imputation would be artificial!
    null_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0]
    to_remove = [col for col in df.columns if (null_counts[col] / n_rows_new) > 0.5]

    df = df.drop(*to_remove)
    log.info(f"Columns with too many missing values removed: {', '.join(to_remove)}")

    # Correct missing values:
    # -----------------------
    # Impute missing values with the most frequent value (mode and median)
    df = impute_with_mode(df)
    df_clean = impute_numerical(df, strategy='median')

    # Report Final Completeness Metric
    Q_cm_rel = compute_relation_completeness(df_clean)
    print(f"\nRelation's Completeness (ratio of complete rows): {Q_cm_rel:.4f}")

    # Sample of final data
    print("\nSample of final data:")
    df_clean.show(5)
    print(f"Enhanced data has {df_clean.count()} rows and {len(df_clean.columns)} columns.")

    # Export df with enhanced quality to PostgreSQL
    # =============================================
    log.info("Exporting DataFrame to PostgreSQL")
    postgres_manager.write_dataframe(df_clean, output_table)
    
    if spark:
        spark.stop()
        log.info("Spark session closed")