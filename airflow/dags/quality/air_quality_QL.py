from dags.utils.postgres_utils import PostgresManager
from dags.landing.class_types import AirStationId
from dags.utils.other_utils import setup_logging
from dags.quality.quality_utils import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from functools import reduce
import os

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

    # 4. Denial Constraints Definition 
    #=================================
    # Available parameters in dataset
    parameter_columns = set(results["numeric_columns"]) - set(["lat", "lon"])
    DC = {}

    # 1. All parameters must be positive
    pos_conditions = [F.col(c) >= 0 for c in parameter_columns]
    DC["pos_params"] = reduce(lambda a, b: a & b, pos_conditions)

    # 2. Check latitude constraint
    if "lat" in df.columns:
        DC["valid_lat"] = (F.col("lat") >= -90) & (F.col("lat") <= 90)

    # 3. Check longitude constraint
    if "lon" in df.columns:
        DC["valid_lon"] = (F.col("lon") >= -180) & (F.col("lon") <= 180)

    # 4. # Chronological timestamps
    window = Window.partitionBy("location").orderBy("datetime_iso")
    DC["ordered_timestamps"] = F.col("datetime_iso") > F.lag("datetime_iso").over(window)

    # 5. PM pyhisical consistency
    if "pm10" in parameter_columns and "pm25" in parameter_columns:
        DC["PM_cons"] = (F.col("pm10") >= F.col("pm25")) | F.col("pm10").isNull() | F.col("pm25").isNull()

    # 6-14. Realistic and expert thresholds for pollutants
    if "pm25" in parameter_columns:
        DC["pm25_realistic"] = (F.col("pm25") < 250) | F.col("pm25").isNull() # PM2.5 - Extreme: 250ug/m3
    if "pm10" in parameter_columns:
        DC["pm10_realistic"] = (F.col("pm10") < 500) | F.col("pm10").isNull() # PM10 - Extreme: 500ug/m3
    if "co" in parameter_columns:
        DC["co_realistic"] = (F.col("co") < 50 )| F.col("co").isNull()  # CO - Extreme: 50ppm
    if "no" in parameter_columns:
        DC["no_realistic"] = (F.col("no") < 10) | F.col("no").isNull()  # NO - Extreme industrial areas
    if "no2" in parameter_columns:
        DC["no2_realistic"] = (F.col("no2") < 1) | F.col("no2").isNull() # NO₂ - 1ppm
    if "nox" in parameter_columns:
        DC["nox_realistic"] = (F.col("nox") < 2) | F.col("nox").isNull() # NOx (NO + NO₂ combined)
    if "o3" in parameter_columns:
        DC["o3_realistic"] = (F.col("o3") < 0.2) | F.col("o3").isNull() # O₃ - Extreme smog: 0.2ppm

    # 15. NOx check ( = NO + NO₂)
    if all(col in parameter_columns for col in ["no", "no2", "nox"]):
        DC["check_NOx"] = (
            (F.col("no").isNull()  | 
             F.col("no2").isNull() | 
             F.col("nox").isNull())| 
            (F.abs(F.col("nox") - (F.col("no") + F.col("no2"))) < 0.001))

    # 16. Ensure at least one parameter is non-null
    DC["non_null_row"] = reduce(
            lambda a, b: a | b, 
            [F.col(c).isNotNull() for c in parameter_columns])
    
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
    log.info("Performing Outlier Detection")

    outlier_df = df.select("datetime_iso")
    for col in parameter_columns:
        # Consider 7 day-window (i.e. 7*12 rows)
        outlier_df = detect_TS_outliers(df, outlier_df, col, window_size=84, timestamp_col="datetime_iso")
    outlier_df = outlier_df.orderBy("datetime_iso")

    # Outliers per row
    # ----------------
    outlier_cols = [c for c in outlier_df.columns if c.startswith("is_outlier_")]

    # Add total outliers column
    outlier_stats = outlier_df.withColumn("total_outliers", sum(F.col(c).cast("int") for c in outlier_cols))
    outlier_stats.orderBy(F.desc("total_outliers")).show(5)

    # Outliers per Attribute
    # ----------------
    param_stats = outlier_df.agg(*[F.sum(F.col(c).cast("integer")).alias(c) for c in outlier_cols]).toPandas().T.reset_index()
    param_stats.columns = ["Parameter", "Total_Outliers"]

    # Calculate outlier percentage
    total_rows = outlier_df.count()
    param_stats["Outlier_Pct"] = (param_stats["Total_Outliers"] / total_rows * 100).round(2)

    print("Outliers by Parameter:")
    print(f"\n{param_stats.sort_values('Total_Outliers', ascending=False).to_string()}")

    # 7. Improve the quality of the data
    #===================================

    # Constraint Enforcing:
    # ---------------------
    log.info("Enforcing Denial Constraints")

    # pos_params: all parameters must be positive -> convert to null if negative
    for col in parameter_columns:
        df = df.withColumn(col, F.when(F.col(col) < 0, None).otherwise(F.col(col)))

    # {paramet  er}_realistic: realistic thresholds must be respected -> convert to null if not
    for DC_name, DC_expr in DC.items():
        if DC_name.endswith("_realistic"):
            # Extract parameter name from constraint name (e.g., "pm25" from "pm25_realistic")
            param = DC_name.replace("_realistic", "")
            df = df.withColumn(param, F.when(DC_expr, F.col(param)).otherwise(None))

    # See: outliers are dealt with this step. Only impossible values are removed, but not extreme values !

    # ordered_timestamps: chronological timestamps
    df = df.orderBy("datetime_iso")

    if "nox" in df.columns:
        # Check NOx consistency (NO + NO₂) -> correct it if possible
        sum_exp = F.col("no") + F.col("no2")
        df = df.withColumn("nox", F.when(F.col("no").isNotNull() & F.col("no2").isNotNull(), sum_exp).otherwise(None))

    # Remove tuples:
    # --------------

    # Duplicates and (aggregation, that has been done with pivot function in formatting)
    df = df.dropDuplicates(["datetime_iso"]); n_rows_new = df.count()
    log.info(f"Duplicates removed: {n_rows - n_rows_new}")

    # Tuples with lots of missing values (i.e. more than 70% of parameters)
    null_count = reduce(lambda a, b: a + b, [F.isnull(col).cast("int") for col in parameter_columns])
    df = df.filter((null_count / len(parameter_columns)) < 0.7)
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
    to_remove = [col for col in df.columns if (null_counts[col] / n_rows_new) > 0.2]

    df = df.drop(*to_remove)
    log.info(f"Columns with too many missing values removed: {', '.join(to_remove)}")

    # Correct missing values:
    # -----------------------
    # Imputation with linear interpolation
    df_clean = impute_with_zero(df)

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
