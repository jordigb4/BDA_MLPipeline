from pyspark.sql import functions as F
from pyspark.sql.types import NumericType
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType
from datetime import timedelta

# ===================
# Profiling Functions
# ===================
def descriptive_profile(df: DataFrame) -> dict:
    """
    Data Profiling function to analyze a Spark dataFrame.
    Returns:
        A dictionary containing overall row count, missing counts per column,
        unique counts per column, and descriptive statistics for numerical columns.
    """

    # 1. Row and Column Count
    total_rows = df.count()
    total_cols = len(df.columns)
    
    # 2. Descriptive Statistics for numeric columns
    num_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, NumericType)]
    stats_num = df.select(num_cols).describe().toPandas() if num_cols else None
    
    # 3. Unique Counts per attribute
    unique_counts = {}
    for col in df.columns:
        count_unique = df.select(col).distinct().count()
        unique_counts[col] = count_unique
    
    # Pack the results into a dictionary
    return {
        "total_rows": total_rows,
        "total_cols": total_cols,
        "numeric_columns": num_cols,
        "descriptive_stats": stats_num,
        "unique_counts": unique_counts,
    }


def print_dataset_profile(results: dict) -> None:
    """
    Prints dataset profile with aligned statistics for multiple variables per row.
    """
    def create_section(title, data, is_stats=False):
        section = [f"{title}:", "-" * 40]
        
        if is_stats:
            variables = sorted(results['numeric_columns'])
            if not variables:
                return "\n".join([*section, "No numeric columns"])
            
            # Prepare column headers
            col_width = 15
            header = "Statistic".ljust(12)
            header += "".join([f"{var:>{col_width}}" for var in variables])
            section.append(header)
            section.append("-" * (12 + col_width * len(variables)))
            
            # Process statistics in standard order
            stats_order = ['count', 'mean', 'stddev', 'min', 'max']
            for stat in stats_order:
                stat_row = data[data['summary'].str.lower() == stat]
                if stat_row.empty:
                    continue
                    
                values = []
                for var in variables:
                    val = stat_row[var].values[0]
                    try:
                        if stat == 'count':
                            formatted = f"{int(float(val))}"
                        else:
                            formatted = f"{float(val):.2f}"
                    except:
                        formatted = str(val)
                    values.append(formatted)
                
                stat_label = f"{stat.capitalize()}:".ljust(12)
                stat_line = stat_label + "".join([f"{v:>{col_width}}" for v in values])
                section.append(stat_line)
            
        else:
            for col in sorted(data.keys()):
                val = data[col]
                line = f"- {col}: {val}"
                section.append(line)
                
        return "\n".join(section)

    # Build output sections
    sections = [
        f"Total Rows: {results['total_rows']} \n Total Columns: {results['total_cols']}",
        create_section("Unique Values Count", results['unique_counts']),
        create_section("Numeric Columns", {"columns": results['numeric_columns']} 
          if results['numeric_columns'] else "No numeric columns")
    ]
    
    if results['descriptive_stats'] is not None:
        sections.append(
            create_section("Descriptive Statistics", 
                          results['descriptive_stats'], 
                          is_stats=True)
        )
    
    return "\n\n".join(sections)

# =========================
# Quality Metrics Functions
# =========================
def compute_column_completeness(df):
    """Calculate the ratio of missing values for each column in the DataFrame."""
    total_rows = df.count()
    if total_rows == 0:
        return df.sql_ctx.createDataFrame([], schema=["column", "missing_ratio"])
    # Calculate the number of nulls for each column
    exprs = [F.sum(F.col(column).isNull().cast("int")).alias(column) for column in df.columns]
    null_counts = df.agg(*exprs).first()
    # Prepare the result as a list of tuples
    stats = [(column, null_counts[column] / total_rows) for column in df.columns]
    # Create a DataFrame from the results
    spark = df.sql_ctx.sparkSession
    return spark.createDataFrame(stats, ["column", "missing_ratio"])


def compute_relation_completeness(df):
    """Calculate the ratio of rows with no missing values in the DataFrame."""
    total_rows = df.count()
    if total_rows == 0 or len(df.columns) == 0:
        return 0.0
    
    # Create an expression that checks all columns are not null
    all_non_null_expr = F.expr(" AND ".join([f"({col} IS NOT NULL)" for col in df.columns]))
    # Calculate the average of the non-null indicator (gives the ratio)
    ratio_row = df.select(F.avg(all_non_null_expr.cast("float"))).first()[0]
    return ratio_row if ratio_row is not None else 0.0


def compute_attribute_timeliness(df: DataFrame, column: str, transactionTime_col: str ,update_rate: float = 12) -> DataFrame:
    """
    Compute attribute-level timeliness scores (Q_T_Ai).
        transactionTime_col: column with the transaction time of the data.
        update rate: assumed same update_rate for all values in a column. Default is 12 updates/day.
    """
    
    # Calculate age in days for each value: assume all values in a row have the same age
    df = df.withColumn("age_days", F.datediff(F.current_timestamp(), F.col(transactionTime_col)) + F.lit(1e-9)) # avoid division by zero
    
    # Calculate timeliness score per value: assume all have the same update rate
    df = df.withColumn("Q_T_v", F.lit(1) / (F.lit(1) + F.col("age_days") * F.lit(update_rate))
    )
    
    # Calculate average score for the attribute
    return df.agg(F.lit(column).alias("attribute"), F.avg("Q_T_v").alias("timeliness_score"))


def interpolate_missing(df):
    """Impute missing values using linear interpolation for numeric columns."""
    # Ensure the DataFrame is sorted by DATE to maintain chronological order
    df = df.orderBy("DATE")

    # Identify numeric columns (exclude DATE)
    numeric_cols = [col for col in df.columns
                    if col != "DATE" and isinstance(df.schema[col].dataType, NumericType)]

    for column in numeric_cols:
        # Define window ordered by DATE (spanning entire dataset)
        window_spec = Window.orderBy("DATE")

        # Previous non-null value (last non-null before current row)
        prev_val = F.last(F.col(column), ignorenulls=True).over(
            window_spec.rowsBetween(Window.unboundedPreceding, -1)
        )

        # Next non-null value (first non-null after current row)
        next_val = F.first(F.col(column), ignorenulls=True).over(
            window_spec.rowsBetween(1, Window.unboundedFollowing)
        )

        # Compute interpolated value
        interpolated = F.when(
            F.col(column).isNull(),
            F.when(
                prev_val.isNotNull() & next_val.isNotNull(),
                (prev_val + next_val) / 2  # Average if both exist
            ).when(
                prev_val.isNotNull(),
                prev_val  # Use previous if only it exists
            ).when(
                next_val.isNotNull(),
                next_val  # Use next if only it exists
            ).otherwise(F.lit(None))  # Leave as null if no values
        ).otherwise(F.col(column))  # Keep original value if not null

        # Update the column with interpolated values
        df = df.withColumn(column, interpolated)

    return df

def check_missing_dates(df: DataFrame, date_col: str = 'datetime_iso') -> dict:
    """
    Checks for missing dates in the dataset based on the 'datetime_iso' column.
    Returns a dictionary containing:
        - 'missing_dates': A list of dates that are missing.
        - 'all_dates': A list of all dates in the range.
        - 'is_complete': Boolean indicating if all dates are present in the range.
    """
    
    # Convert 'datetime_iso' to date type
    df = df.withColumn('date_only', F.to_date(F.col(date_col), 'yyyy-MM-dd'))
    
    # Get the minimum and maximum date from the column
    min_date = df.select(F.min('date_only')).first()[0]
    max_date = df.select(F.max('date_only')).first()[0]
    print("Range of dates: ", min_date,max_date)
    
    # Generate a sequence of dates from min_date to max_date using Python's timedelta
    date_range = []
    current_date = min_date
    while current_date <= max_date:
        date_range.append(current_date)
        current_date += timedelta(days=1)
    
    # Create a DataFrame with all dates in the range
    all_dates_df = df.sparkSession.createDataFrame([(d,) for d in date_range], ['date_only'])
    # Get the dates present in the dataset
    present_dates_df = df.select('date_only').distinct()
    # Find missing dates 
    missing_dates_df = all_dates_df.join(present_dates_df, on='date_only', how='left_anti')
    # Collect the missing dates
    missing_dates = [row['date_only'] for row in missing_dates_df.collect()]
    # Check if all dates are present
    is_complete = len(missing_dates) == 0
    
    return {
        'missing_dates': missing_dates,
        'all_dates': date_range,
        'is_complete': is_complete
    }