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

    # Filter data from the last 2 years recorded
    recent_data = df.tail(730)

    # Identify columns with any NA in recent data
    columns_to_drop = [col for col in recent_data.columns
                       if col != "DATE" and recent_data.filter(F.col(col).isNull()).count() > 730*0.2]

    # Drop these columns from the original DataFrame
    filtered_df = df.drop(*columns_to_drop)

    # Interpolate data
    



    # Compute completeness per column




    # Compute completeness per all dataframe



    # filtered_df.write \
    #     .format("jdbc") \
    #     .option("url", "jdbc:postgresql://localhost:5432/your_database") \
    #     .option("dbtable", "element_data") \
    #     .option("user", "your_user") \
    #     .option("password", "your_password") \
    #     .mode("overwrite") \
    #     .save()

    spark.stop()