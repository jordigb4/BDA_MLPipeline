from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import NumericType

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


def compute_row_completeness(df):
    """Calculate the ratio of rows with no missing values in the DataFrame."""
    total_rows = df.count()
    if total_rows == 0:
        return 0.0
    # Create an expression that checks all columns are not null
    if len(df.columns) == 0:
        return 0.0
    all_non_null_expr = F.expr(" AND ".join([f"({col} IS NOT NULL)" for col in df.columns]))
    # Calculate the average of the non-null indicator (gives the ratio)
    ratio_row = df.select(F.avg(all_non_null_expr.cast("float"))).first()[0]
    return ratio_row if ratio_row is not None else 0.0
