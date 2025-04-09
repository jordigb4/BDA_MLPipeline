import logging
import os

from dags.utils.landing.class_types import WeatherStationId
from dags.utils.postgres_utils import PostgresManager
from dags.utils.quality.quality_utils import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def quality_weather(postgres_manager: PostgresManager):
    """
    Improves and analises quality of all three stations.
    """

    # Input tables
    long_beach_table_i = f"formatted_{WeatherStationId.LONG_BEACH.name}"
    downtown_table_i = f"formatted_{WeatherStationId.DOWNTOWN.name}"
    reseda_table_i = f"formatted_{WeatherStationId.RESEDA.name}"

    # Output tables
    long_beach_table_o = f"trusted_{WeatherStationId.LONG_BEACH.name}"
    downtown_table_o = f"trusted_{WeatherStationId.DOWNTOWN.name}"
    reseda_table_o = f"trusted_{WeatherStationId.RESEDA.name}"

    # Call to quality function
    quality_station_weather(long_beach_table_i, long_beach_table_o, postgres_manager)
    quality_station_weather(downtown_table_i, downtown_table_o, postgres_manager)
    quality_station_weather(reseda_table_i, reseda_table_o, postgres_manager)


def quality_station_weather(input_table: str, output_table: str, postgres_manager: PostgresManager) -> None:
    # Initialize Spark session
    spark = SparkSession.builder \
        .config("spark.jars", os.getenv('JDBC_URL')) \
        .appName("DataQualityPipeline") \
        .getOrCreate()

    # 1. Read the table from PostgreSQL

    df = postgres_manager.read_table(spark, input_table)

    # 2. Drop columns with NA in the last 2 years

    # Filter data from the last 2 years recorded
    recent_data = df.orderBy(F.desc("DATE")).limit(730)

    # Identify columns with any NA in recent data
    columns_to_drop = [col for col in recent_data.columns
                       if col != "DATE" and recent_data.filter(F.col(col).isNull()).count() > 730 * 0.2]

    # Drop these columns from the original DataFrame
    filtered_df = df.drop(*columns_to_drop)

    # Interpolate data (imputation)
    interpolated_df = interpolate_missing(filtered_df)

    # Compute completeness stats on the interpolated DataFrame
    column_completeness_df = compute_column_completeness(interpolated_df)
    logging.info("Missing ratios per column after interpolation:")
    column_completeness_df.show(truncate=False)

    row_completeness_ratio = compute_relation_completeness(interpolated_df)
    logging.info(f"\nRatio of rows with no missings after interpolation: {row_completeness_ratio:.4f}")

    # Write dataframe
    postgres_manager.write_dataframe(interpolated_df, output_table)

    spark.stop()
