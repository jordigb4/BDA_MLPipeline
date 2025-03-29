from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import glob

def format_weather():
    # Initialize Spark session
    spark = SparkSession.builder \
        .config("spark.jars", "/opt/airflow/postgresql-42.7.3.jar") \
        .appName("ElementDataFormatter") \
        .getOrCreate()

    # Read Parquet files
    df = spark.read.parquet("hdfs://namenode:8020/data/landing/weather/DOWNTOWN")

    print("Initial Schema:")
    df.printSchema()
    print("\nInitial Sample Data:")
    df.show(5, truncate=False)


    # Convert DATE to standard date format (assuming 'yyyyMMdd' string)
    df = df.withColumn("DATE", F.to_date(F.col("DATE"), "yyyyMMdd"))

    # Pivot to create columns for each element
    pivoted_df = df.groupBy("DATE").pivot("ELEMENT").agg(F.first("DATA_VALUE"))

    pivoted_df = pivoted_df.orderBy("DATE",ascending=True)

    print("Final Schema:")
    pivoted_df.printSchema()
    print("\nSample Data:")
    pivoted_df.show(5, truncate=False)

    # Write to PostgreSQL (update connection details as needed)
    pivoted_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow") \
        .option("dbtable", "element_data") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    spark.stop()