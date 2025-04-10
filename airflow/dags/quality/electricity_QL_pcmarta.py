
import os
import logging
from quality_utils import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configura el logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
)

log = logging.getLogger(__name__)


    
def quality_electricity(input_table: str, output_table: str) -> None:
    """
    Executes the whole Data Quality Pipeline for one particular air station.
    """

    # Initialize Spark session
    spark = SparkSession.builder \
            .appName("ElectricityDataQuality") \
            .master("local[*]") \
            .getOrCreate()
            
    # Set the legacy time parser policy to avoid the date parsing error
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    # 1. Read the table from PostgreSQL
    # =================================
    #df = postgres_manager.read_table(spark, input_table)
    df = spark.read.parquet(input_table)
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

    # Check if there are missing dates
    missing_dates_result = check_missing_dates(df)

    # Display results for missing dates
    if missing_dates_result['is_complete']:
        print("\nAll dates are present in the dataset.")
    else:
        print(f"\nMissing dates: {', '.join(str(date) for date in missing_dates_result['missing_dates'])}")
        print(f"Total dates in the range: {len(missing_dates_result['all_dates'])}")
        print(f"Dates present in the dataset: {len(missing_dates_result['all_dates']) - len(missing_dates_result['missing_dates'])}")

    # 4. Denial Constraints Definition 
    #=================================

    DC = {
        "pos_value": F.col("value") >= 0,  # Ensure that the value is positive
        "valid_date": F.to_date(F.col("date"), "yyyy-MM-dd").isNotNull(), 
        "valid_datetime_iso": F.col("datetime_iso").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00$"),  
        #"ordered_timestamps": F.col("datetime_iso") > F.lag("datetime_iso").over(Window.orderBy("datetime_iso")),  # Ensure dates are in order in 'datetime_iso'
        #"ordered_dates": F.col("date") > F.lag("date").over(Window.orderBy("date")),  # Ensure dates are in order in 'date'
        "value_realistic": F.col("value") <= 500000,  
        "valid_unit_of_measure": F.col("unit_of_measure").isin("mwh"),  # Only one unit of measure possible
        #"consistent_respondent_name": F.countDistinct("respondent_name").over(Window.partitionBy("respondent_id")) == 1,  # Ensure all rows for a respondent_id have the same respondent_name
        "valid_timezone": F.col("timezone_id").isin("pacific", "eastern", "central"),  # Valid timezone
    }

 
   
    print(DC)
    # 4. Apply the denial constraints
    #================================
    # Apply the constraints one by one
    for constraint_name, constraint_expr in DC.items():
        log.info(f"Applying constraint: {constraint_name}")
        df = df.filter(constraint_expr)  # Keep only rows that meet the condition



    if spark:
        spark.stop()
        log.info("Spark session closed")


input_table = "C:/Users/marta/formatted_data.parquet"
output_table = "C:/Users/marta/quality_data.parquet"


quality_electricity(input_table, output_table)
