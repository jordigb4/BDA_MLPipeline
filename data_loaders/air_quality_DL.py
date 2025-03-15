import subprocess
import logging
import os

from dateutil.relativedelta import relativedelta
from datetime import datetime

from dotenv import load_dotenv
from class_types import AirStationId

# Configure logging
logging.basicConfig(
    level=logging.INFO, # minimum logging level
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
    # filename = 'app.log' directs log messages to that file
    # filemode = 'a' (append) or 'w' (write/overwrite): mode of opening log file
    # stream = sys.stdout  -> directs log messages to standard output
)

load_dotenv()

def load_data_air(station_id: AirStationId, start_date: str, end_date: str, output_dir: str = './data'):
    """
    Download OpenAQ data from an Air station using AWS CLI subprocess, from a given start date to an end date.
    
    Args:
        station_id (AirStationId): Air station identifier.
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.
        output_dir (str): Base output directory.
    
    Result:
        Downloads the data from the OpenAQ S3 bucket to the output directory (.csv.gz files).
    """
    # Validate date format
    try:
        # Validate date format and order
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        if start > end:
            raise ValueError("Start date cannot be after end date")

    except ValueError as e:
        logging.error(f"Invalid date format: {str(e)}")
        raise

    logging.info("=" * 40)
    logging.info("Processing location: %s", station_id.name)
    logging.info("=" * 40)

    current = start
    while current <= end:
        # Generate date components
        year = current.strftime('%Y')
        month = current.strftime('%m')
        
        # Build paths
        path_to_save = os.path.join(output_dir, station_id.name, year, month)
        os.makedirs(path_to_save, exist_ok=True)

        # Build AWS CLI command
        s3_prefix = os.getenv('S3_PREFIX_AIR_QUALITY').format(
            station_id=station_id.value, year=year, month=month
        )
        cmd = [
            'aws', 's3', 'sync',
            s3_prefix,
            path_to_save,
            '--exclude', '*',
            '--include', '*.csv.gz',
            '--no-sign-request',
            '--source-region', 'us-east-1',
            '--region', 'us-east-1'
        ]

        logging.info("Downloading %s...", current.strftime('%Y-%m'))

        # Execute command
        result = subprocess.run(cmd, capture_output=True, text=True)  # better without shell=True for security reasons

        # Handle command output
        if result.returncode == 0:
            if "download:" in result.stdout:
                num_files = result.stdout.count("download:")
                logging.info("Downloaded %d new files", num_files)
            else:
                logging.info("No new files to download")
        else:
            logging.error("Error processing %s", s3_prefix)
            logging.error("Exit code: %d", result.returncode)
            logging.error("Error message:\n%s", result.stderr)

        # Move to next month
        current += relativedelta(months=+1)

if __name__ == "__main__":
    # Example usage
    load_data_air(
        station_id=AirStationId.RESEDA,
        start_date='2019-01-01',
        end_date='2019-03-31',
        output_dir='./air_data'
    )
