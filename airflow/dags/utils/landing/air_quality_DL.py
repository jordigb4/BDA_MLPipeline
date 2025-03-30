from concurrent.futures import ThreadPoolExecutor, as_completed
from dags.utils.landing.class_types import AirStationId
from dags.utils.hdfs_utils import HDFSManager
from datetime import datetime
from pathlib import Path
import subprocess
import tarfile
import logging
import time
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
)

log = logging.getLogger(__name__)

# Resilient S3 Sync Function
def aws_s3_sync(cmd, max_retries=3, retry_delay=2):
    """Robust S3 sync with error handling"""
    for attempt in range(max_retries):
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            return result
        elif "No files or objects found" in result.stderr:
            return result  # Not an error - just no files
        elif attempt < max_retries - 1:
            log.warning(f"Retry {attempt+1}/{max_retries} for S3 sync")
            time.sleep(retry_delay * (attempt + 1))
    
    # Final failure
    raise subprocess.CalledProcessError(
        result.returncode, cmd, 
        output=result.stdout, 
        stderr=result.stderr
    )


def load_data_air(hdfs_manager: HDFSManager, start_date: str, end_date: str):
    """
    Loads data from all three selected stations
    """

    # Create base temp directory if it doesn't exist
    tmp_base_dir = Path('/tmp/traffic_acc')
    tmp_base_dir.mkdir(parents=True, exist_ok=True)

    try:
        stations = [AirStationId.LONG_BEACH, AirStationId.DOWNTOWN, AirStationId.RESEDA]
        with ThreadPoolExecutor(max_workers=3) as executor:
            # Track futures explicitly
            futures = {
                executor.submit(
                    load_station_data,
                    station, start_date, end_date, hdfs_manager
                ): station for station in stations}
            
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
        subprocess.run(["rm", "-rf", '/tmp/air_quality/'], check=True)


def load_station_data(station_id: AirStationId, 
                      start_date: str, 
                      end_date: str, 
                      hdfs_manager: HDFSManager):
    """
    Download OpenAQ data for a station by year using AWS CLI subprocess.
    For each year, merge all downloaded files into a single archive and upload to HDFS.
    """
    # ===== DATE VALIDATION =====
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        if end < start:
            log.error(f"end_date {end_date} precedes start_date {start_date}")
            raise ValueError("End date precedes start date")
    except ValueError as e:
        log.error(f"Invalid date format: {str(e)}")
        raise

    # ==== PATH CONFIGURATION ====
    base_tmp_dir = f'/tmp/air_quality/{station_id.name}'
    hdfs_dir = f"/data/landing/air_quality"

    # Process year-by-year
    for year in range(start.year, end.year + 1):
        year_str = f"{year:04}"
        yearly_tmp_dir = Path(f"{base_tmp_dir}/{year_str}")
        yearly_tmp_dir.mkdir(parents=True, exist_ok=True)

        # Determine month range for this year
        start_month = start.month if year == start.year else 1
        end_month = end.month if year == end.year else 12

        # Build AWS include filters for each month
        include_args = []
        for month in range(start_month, end_month + 1):
            include_args.extend(['--include', f'*month={month:02}*.csv.gz'])

        # Build S3 prefix from environment variable
        s3_prefix = os.getenv('S3_PREFIX_AIR_QUALITY').format(
            station_id=station_id.value, year=year_str)

        # ===== AWS CLI QUERY =====
        cmd = [
            'aws', 's3', 'sync',
            s3_prefix,
            str(yearly_tmp_dir),
            '--exclude', '*',
            *include_args,
            '--no-sign-request',
            '--source-region', 'us-east-1',
            '--region', 'us-east-1'
        ]

        # ===== DATA EXTRACTION =====
        log.info(f"Downloading S3 data for {station_id} year {year_str} (months {start_month}-{end_month})")
        start_time = datetime.now()
        try:
            # Use resilient sync function
            result = aws_s3_sync(cmd)
            
            # Handle results
            if "No files or objects found" in result.stderr:
                log.warning(f"No files found for {station_id} {year}")
            else:
                num_files = result.stdout.count("download:")
                log.info(f"Retrieved {num_files} files for year {year_str} in {(datetime.now()-start_time).total_seconds():.2f}s")
        except subprocess.CalledProcessError as e:
            log.error(f"S3 sync failed: {e.stderr}")
            raise

        # ===== HDFS STORAGE =====
        if num_files > 0:
            
            # Merge files -> reduce writings in HDFS
            output_file = f"{base_tmp_dir}/{year_str}.tar.gz"
            with tarfile.open(output_file, "w:gz") as tar:

                # Walk through the yearly directory and add all files
                for root, _, files in os.walk(yearly_tmp_dir):
                    for file in files:
                        full_path = os.path.join(root, file)
                        arcname = os.path.relpath(full_path, yearly_tmp_dir)
                        tar.add(full_path, arcname=arcname)       
            try:
            # ===== HDFS STORAGE =====
                log.info(f"Transferring to HDFS: {hdfs_dir}")
                hdfs_manager.copy_from_local(str(output_file), f"{hdfs_dir}/{station_id.name}")
            except Exception as e:
                log.error(f"HDFS transfer failed: {str(e)}")
                raise
        else:
            log.warning(f"No records found for {station_id} in year {year_str} - skipping HDFS transfer")