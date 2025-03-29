from dateutil.relativedelta import relativedelta
from .class_types import AirStationId
from .hdfs_utils import HDFSManager
from datetime import datetime
from pathlib import Path
import subprocess
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO, # minimum logging level
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
    # filename = 'app.log' directs log messages to that file
    # filemode = 'a' (append) or 'w' (write/overwrite): mode of opening log file
    # stream = sys.stdout  -> directs log messages to standard output
)

# Create a module-specific logger
log = logging.getLogger(__name__)

def load_data_air(hdfs_manager: HDFSManager,
                      start_date: str,
                      end_date: str):
    """
    Loads data from all three selected stations
    """
    
    load_station_data(AirStationId.LONG_BEACH, start_date, end_date, hdfs_manager)
    load_station_data(AirStationId.DOWNTOWN, start_date, end_date, hdfs_manager)
    load_station_data(AirStationId.RESEDA, start_date, end_date, hdfs_manager)

    # Clear temporal files of previous data collections
    subprocess.run(["rm", "-rf", '/tmp/air_quality/'], check=True)


def load_station_data(station_id: AirStationId, 
                  start_date: str, 
                  end_date: str, 
                  hdfs_manager: HDFSManager):
    """
    Download OpenAQ data from an Air station using AWS CLI subprocess, from a given start date to an end date.
    
    Args:
        station_id: Station identifier with name/value attributes
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        hdfs_manager: HDFS management client for final storage
    
    Result:
        Downloads the data from the OpenAQ S3 bucket to the output directory (.csv.gz files).
    """
    # ===== DATE VALIDATION =====
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        if end < start:
            log.error(f"end_date {end_date} precedes start_date {start_date}")
            raise
    except ValueError as e:
        log.error(f"Invalid date format: {str(e)}")
        raise

    # ===== PATH CONFIGURATION =====
    tmp_dir = f'/tmp/air_quality/{station_id.name}'
    hdfs_dir = "/data/landing/air_quality"

    current = start
    while current <= end:
        # Generate date components
        year = current.strftime('%Y')
        month = current.strftime('%m')
        
        # Build relative path
        rel_path = Path(f"{tmp_dir}/{year}/{month}")
        rel_path.mkdir(parents=True, exist_ok=True)

        # ===== BUILD AWS CLI COMMAND =====
        s3_prefix = os.getenv('S3_PREFIX_AIR_QUALITY').format(
            station_id=station_id.value, year=year, month=month
        )
        cmd = [
            'aws', 's3', 'sync',
            s3_prefix,
            rel_path,
            '--exclude', '*',
            '--include', '*.csv.gz',
            '--no-sign-request',
            '--source-region', 'us-east-1',
            '--region', 'us-east-1'
        ]

        # ===== EXECUTION PHASE =====
        log.info(f"Downloading S3 data for {station_id} from {current.strftime('%Y-%m')}")
        start_time = datetime.now()
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)  # better without shell=True for security reasons
            num_files = result.stdout.count("download:")
            log.info(f"Retrieved {num_files} files in "
                        f"{(datetime.now()-start_time).total_seconds():.2f}s")   
        except subprocess.CalledProcessError as e:
            log.error(f"S3 sync failed: {e.stderr}")
            raise

        # Move to next month
        current += relativedelta(months=+1)
    
    # ===== HDFS STORAGE =====
    if num_files > 0:
        log.info(f"Transferring to HDFS: {hdfs_dir}")
        hdfs_manager.copy_from_local(tmp_dir, hdfs_dir)
    else:
        log.warning("No records found - skipping HDFS transfer")