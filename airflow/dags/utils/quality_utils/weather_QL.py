from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def quality_data():
    # Initialize Spark session
    spark = SparkSession.builder \
        .config("spark.jars", "/opt/airflow/postgresql-42.7.3.jar") \
        .appName("DataQualityPipeline") \
        .getOrCreate()

    # 1. Read the table from PostgreSQL (update connection details)

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow") \
        .option("dbtable", "element_data") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .load()

    # 2. Drop columns with NA in the last 2 years
    current_date = F.current_date()
    two_years_ago = F.date_sub(current_date, 730)  # ~2 years

    # Filter data from the last 2 years
    recent_data = df.filter(F.col("DATE") >= two_years_ago)

    # Identify columns with any NA in recent data
    columns_to_drop = [col for col in recent_data.columns
                       if col != "DATE" and recent_data.filter(F.col(col).isNull()).count() > 0]

    # Drop these columns from the original DataFrame
    filtered_df = df.drop(*columns_to_drop)

    # 3. Compute completeness metrics and raise alerts
    # Column completeness: ratio of non-null values per column
    column_completeness = {}
    for col in filtered_df.columns:
        if col == "DATE":
            continue
        non_null_count = filtered_df.filter(F.col(col).isNotNull()).count()
        total_rows = filtered_df.count()
        completeness = non_null_count / total_rows if total_rows > 0 else 0
        column_completeness[col] = completeness

    # Row completeness: ratio of non-null columns per row
    total_columns = len(filtered_df.columns) - 1  # Exclude DATE
    row_completeness_df = filtered_df.withColumn(
        "non_null_count",
        sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in filtered_df.columns if c != "DATE")
    ).withColumn(
        "row_completeness",
        F.col("non_null_count") / F.lit(total_columns)
    )

    # Define thresholds (adjust as needed)
    COLUMN_COMPLETENESS_THRESHOLD = 0.8  # Alert if column completeness < 80%
    ROW_COMPLETENESS_THRESHOLD = 0.5     # Alert if row completeness < 50%

    # Raise alerts for columns
    for col, comp in column_completeness.items():
        if comp < COLUMN_COMPLETENESS_THRESHOLD:
            print(f"Airflow Alert: Column '{col}' completeness is {comp:.2f} (below {COLUMN_COMPLETENESS_THRESHOLD})")

    # Raise alerts for rows (example: log rows with low completeness)
    low_completeness_rows = row_completeness_df.filter(F.col("row_completeness") < ROW_COMPLETENESS_THRESHOLD)
    if low_completeness_rows.count() > 0:
        print(f"Airflow Alert: Found {low_completeness_rows.count()} rows with completeness < {ROW_COMPLETENESS_THRESHOLD}")
        low_completeness_rows.select("DATE", "row_completeness").show()

    # 4. Forward linear interpolation (using forward fill as a proxy)
    window_spec = Window.orderBy("DATE")

    # Apply forward fill to remaining columns
    for col in filtered_df.columns:
        if col != "DATE":
            filtered_df = filtered_df.withColumn(
                col,
                F.last(col, ignorenulls=True).over(window_spec.rowsBetween(Window.unboundedPreceding, 0))
            )

    # Show result (optional)
    filtered_df.show()

    # Write interpolated data back to PostgreSQL (optional)
    # filtered_df.write \
    #     .format("jdbc") \
    #     .option("url", "jdbc:postgresql://localhost:5432/your_database") \
    #     .option("dbtable", "element_data_interpolated") \
    #     .option("user", "your_user") \
    #     .option("password", "your_password") \
    #     .mode("overwrite") \
    #     .save()

    spark.stop()