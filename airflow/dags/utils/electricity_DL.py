from .hdfs_utils import HDFSManager
from datetime import datetime
from pathlib import Path
import requests
import logging
import json
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO, # minimum logging level
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
)
# Create a module-specific logger
log = logging.getLogger(__name__)


def load_data_electricity(start_date: str,
                          end_date: str,
                          hdfs_manager: HDFSManager):
    """
    Electricity data loader with API integration and HDFS storage
    
    Args:
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        hdfs_manager: HDFS management client for final storage
    """
    # ===== DATE VALIDATION =====
    try:
        # Clean date strings first
        clean_start = start_date.split('T')[0]
        clean_end = end_date.split('T')[0]
        
        start_dt = datetime.strptime(clean_start, '%Y-%m-%d')
        end_dt = datetime.strptime(clean_end, '%Y-%m-%d')
        if start_dt > end_dt:
            raise ValueError("Start date after end date")
    except ValueError as e:
        log.error(f"Date validation failed: {str(e)}")
        raise

    # ===== PATH CONFIGURATION =====
    tmp_dir = Path("/tmp/electricity")
    hdfs_dir = "/data/landing/electricity"

    tmp_dir.mkdir(parents=True, exist_ok=True)
    output_file = tmp_dir / f"{start_date.replace('-', '')}_{end_date.replace('-', '')}.json"

    # ===== API CONFIGURATION =====
    api_key = os.getenv('API_KEY_ELECTRICITY')
    base_url = os.getenv('API_DOMAIN_ELECTRICITY')
    
    # ===== REQUEST PARAMETERS =====
    params = {
        "api_key": api_key,
        "frequency": "hourly",
        "data[0]": "value",
        "facets[respondent][]": "LDWP",
        "start": start_date,
        "end": end_date,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000
    }
    headers = {"Content-Type": "application/json"}

    # ===== DATA EXTRACTION =====
    try:
        log.info(f"Requesting electricity data ({start_date} to {end_date})")
        start_time = datetime.now()
        response = requests.get(base_url, headers=headers, params=params, timeout=30) #prevent hanging requests with timeout
        duration = datetime.now() - start_time
        
        if response.status_code != 200:
            log.error(f"API request failed: {response.status_code} - {response.text[:200]}")
            raise

        data = response.json()
        if not data.get('response', {}).get('data'):
            log.warning("No electricity data found for given parameters")
            return

        # ===== LOCAL STORAGE =====
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2) # balance indent between compactness and readability
        log.info(f"Saved {len(data['response']['data'])} records in {duration.total_seconds():.2f}s")

        # ===== HDFS STORAGE =====
        try:
            log.info(f"Transferring to HDFS: {hdfs_dir}")
            hdfs_manager.copy_from_local(output_file, hdfs_dir)
        except Exception as e:
            log.error(f"HDFS transfer failed: {str(e)}")
            raise

    except requests.exceptions.RequestException as e:
        log.error(f"API connection failed: {str(e)}")
        raise
