import os
import plotly.express as px
from dags.landing.class_types import WeatherStationId
from dags.utils.hdfs_utils import HDFSManager
from dags.utils.postgres_utils import PostgresManager
from pyspark.sql import SparkSession
import io
import pickle
import logging


def data_analysis_1(hdfs_manager: HDFSManager, postgres_manager: PostgresManager, input_view: str = 'experiment1'):
    # Spark session
    spark = SparkSession.builder \
        .config("spark.jars", os.getenv('JDBC_URL')) \
        .appName("WeatherQuality") \
        .getOrCreate()

    # Base path for experiments
    hdfs_path = '/data/data_analysis/exp1/'
    logging.info(f'Making directories {hdfs_path}')

    # Ensure the directory exists
    hdfs_manager.mkdirs(hdfs_path)

    # Read view
    df = postgres_manager.read_table(spark, input_view)

    # Generate a figure for each station
    for station in WeatherStationId:
        fig = plot_exp_1(df, station.name.lower())

        # Serialize figure to an in-memory bytes buffer
        buffer = io.BytesIO()
        pickle.dump(fig, buffer)
        buffer.seek(0)

        # Define HDFS file path
        hdfs_file_path = hdfs_path + f"{station.name.lower()}.pkl"

        logging.info(f"Saving exp1_{station.name.lower()}.pkl to {hdfs_file_path}")

        # Upload buffer content directly to HDFS
        with hdfs_manager._client.write(hdfs_file_path, overwrite=True) as writer:
            writer.write(buffer.read())

def plot_exp_1(df, selected_station):
    # --- Filter and convert to Pandas ---
    filtered_df = df.filter(df['station'] == selected_station).toPandas()

    # --- Create Scatter Matrix Plot ---
    fig = px.scatter_matrix(
        filtered_df,
        dimensions=['TMAX', 'TMIN', 'PRCP'],
        color='cenergy',
        title=f'Weather vs Consumed Energy for {selected_station}',
        labels={col: col for col in ['TMAX', 'TMIN', 'PRCP', 'cenergy']},
        height=700
    )

    # Update marker size for better visibility
    fig.update_traces(diagonal_visible=False, showupperhalf=False, marker=dict(size=5))

    return fig
