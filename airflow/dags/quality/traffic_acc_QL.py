import logging
import os

from dags.landing.class_types import TrafficAccId
from dags.utils.postgres_utils import PostgresManager
from dags.quality.quality_utils import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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
    df = postgres_manager.read_table(spark, input_table)

    logging.info(f"Read {input_table} from PostgreSQL")
    df.show(5)

    spark.stop()
