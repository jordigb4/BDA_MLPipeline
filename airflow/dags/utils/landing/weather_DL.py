from dags.utils.landing.class_types import WeatherStationId
from concurrent.futures import ThreadPoolExecutor, as_completed
from dags.utils.hdfs_utils import HDFSManager
from datetime import datetime
from pathlib import Path
import subprocess
import logging
import duckdb
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO, # minimum logging level
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S')

# Create a module-specific logger
log = logging.getLogger(__name__)


def load_data_weather(hdfs_manager: HDFSManager, start_date: str, end_date: str):

    try:
        stations = [WeatherStationId.LONG_BEACH, WeatherStationId.DOWNTOWN, WeatherStationId.RESEDA]
        with ThreadPoolExecutor(max_workers=3) as executor:
            # Track futures explicitly
            futures = {
                executor.submit(
                    load_station_data,
                    station, start_date, end_date, hdfs_manager
                ): station for station in stations
            }
            
            # Properly handle completion and exceptions
            for future in as_completed(futures):
                station = futures[future]
                try:
                    future.result()  # Raises exceptions if any occurred
                except Exception as e:
                    log.error(f"Station {station} failed: {str(e)}")
                    raise
    finally:
        # Cleanup only after ALL tasks complete
        subprocess.run(["rm", "-rf", '/tmp/weather/'], check=True)
        inspect_hdfs_structure(hdfs_manager, '/data')


def load_station_data(station_id: WeatherStationId,
                     start_date: str,
                     end_date: str,
                     hdfs_manager: HDFSManager):
    """
    Weather data collector with S3 querying, date validation, and HDFS storage.
    Combines direct S3 filtering via DuckDB with HDFS storage capabilities.

    Args:
        station_id: Station identifier with name/value attributes
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        hdfs_manager: HDFS management client for final storage

    Result:
        Downloads the data from the OpenAQ S3 bucket to the HDFS system (parquet files).
    """
    
    # ===== DATE VALIDATION =====
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        if end_dt < start_dt:
            log.error(f"end_date {end_date} precedes start_date {start_date}")
            raise
    except ValueError as e:
        log.error(f"Invalid date format: {str(e)}")
        raise

    # ===== PATH CONFIGURATION =====
    tmp_dir = Path(f'/tmp/weather/{station_id.name}')
    hdfs_dir = "/data/landing/weather"

    tmp_dir.mkdir(parents=True, exist_ok=True)
    output_file = tmp_dir / f"{start_date}_{end_date}.parquet"

    # ===== DUCKDB S3 QUERY =====
    try:
        with duckdb.connect() as conn:
            # Configure S3 access
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            conn.execute("SET s3_region='us-east-1'; SET s3_use_ssl=false;")

            # Build S3 source path from environment variable
            s3_source = os.getenv('S3_PREFIX_WEATHER').format(station_id=station_id.value)
            
            # Core data columns
            columns = ['STATION', 'DATE', 'OBS_TIME', 'ELEMENT', 'DATA_VALUE']
            column_list = ', '.join(columns)

            # ===== MAIN QUERY =====
            # -- Create temporal table to copy then in output_file in parquet format.
            # -- Select only the columns with useful information from s3_source w. data for station_id.
            # -- Filter that rows dated out of the range.
            query = f"""
            CREATE TEMP TABLE station_data AS
            SELECT {column_list}, strptime(CAST(DATE AS STRING), '%Y%m%d')::DATE AS parsed_date
            FROM read_parquet('{s3_source}', hive_partitioning=1, filename=false)
            WHERE parsed_date BETWEEN '{start_date}'::DATE AND '{end_date}'::DATE;
            COPY station_data TO '{output_file}' (FORMAT PARQUET);
            """

            # ===== EXECUTION PHASE =====
            log.info(f"Querying S3 data for {station_id.name}")
            start_time = datetime.now()
            conn.execute(query)
            duration = datetime.now() - start_time

            # Get and log record count
            record_count = conn.sql("SELECT COUNT(*) FROM station_data").fetchone()[0]
            log.info(f"Retrieved {record_count:,} records in {duration.total_seconds():.2f}s")

            # ===== HDFS STORAGE =====
            if record_count > 0:
                log.info(f"Transferring to HDFS: {hdfs_dir}")
                hdfs_manager.copy_from_local(str(tmp_dir), hdfs_dir)
            else:
                log.warning("No records found - skipping HDFS transfer")

    except duckdb.Error as e:
        error_msg = str(e).lower()
        if 'file not found' in error_msg or 'failed to open' in error_msg or 'no such file' in error_msg:
            log.warning(f"S3 source '{s3_source}' does not exist. Skipping {station_id.name}.")
        else:
            log.error(f"DuckDB operation failed: {str(e)}")
            raise
    except Exception as e:
        log.error(f"HDFS transfer failed: {str(e)}")
        raise

def inspect_hdfs_structure(hdfs_manager, path, indent=""):
    """
    Recursively inspect HDFS starting at `path` and print out directories and file names.
    """
    try:
        items = hdfs_manager.list_files(path)
    except Exception as e:
        print(f"Error listing {path}: {e}")
        return

    print(f"{indent}{path}/")
    for item in items:
        item_path = os.path.join(path, item)
        # Using the underlying client's status method to check if this is a directory.
        try:
            status = hdfs_manager._client.status(item_path, strict=False)
        except Exception as e:
            print(f"Error getting status of {item_path}: {e}")
            continue

        # Check if the item is a directory (the status dict usually has a 'type' key).
        if status and status.get('type') == 'DIRECTORY':
            inspect_hdfs_structure(hdfs_manager, item_path, indent + "    ")
        else:
            print(f"{indent}    {item}")