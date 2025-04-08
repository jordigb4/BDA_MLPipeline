#from dags.utils.hdfs_utils import HDFSManager
from datetime import datetime
from pathlib import Path
import requests
import logging
import json
import os
import pandas as pd

os.environ['API_KEY_ELECTRICITY'] = 'QaQfVtyl0eLAi55ZcvMgTcf0eyNASZsGTlBuUsUS'
os.environ['API_DOMAIN_ELECTRICITY'] = 'https://api.eia.gov/v2/electricity/rto/daily-region-data/data/'


# Configure logging
logging.basicConfig(
    level=logging.INFO, # minimum logging level
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
)
# Create a module-specific logger
log = logging.getLogger(__name__)


def load_data_electricity(start_date: str,
                          end_date: str,):
    #                      hdfs_manager: HDFSManager):
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
    #hdfs_dir = "/data/landing/electricity"

    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    output_file = tmp_dir / f"{clean_start}_{clean_end}.json"

    # ===== API CONFIGURATION =====
    api_key = os.getenv('API_KEY_ELECTRICITY')
    base_url = os.getenv('API_DOMAIN_ELECTRICITY')
    offset = 0
    length = 5000 # maximum length request
    
    # ===== DATA EXTRACTION =====
    start_time = datetime.now()
    log.info(f"Requesting electricity data ({start_date} to {end_date})")

    all_data = []
    while True:
        params = {
            "api_key": api_key,
            "frequency": "daily",
            "data[0]": "value",
            "facets[respondent][]": "LDWP",
            "start": start_date,
            "end": end_date,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "offset": offset,
            "length": length
        }

        try:   
            response = requests.get(base_url, params=params, timeout=30) #prevent hanging requests with timeout

            if response.status_code != 200:
                log.error(f"API request failed: {response.status_code} - {response.text[:200]}")
                raise
            
            data = response.json()
            if not data.get('response', {}).get('data'):
                log.warning("No electricity data found for given parameters")
                return
            else:
                filtered_data = []
                for entry in data.get("response", {}).get("data", []):
                    if entry.get("type-name") == "Demand" and entry.get("timezone") == "Pacific":
                        entry.pop("type-name", None)
                        entry.pop("timezone-description", None)
                        filtered_data.append(entry)

                all_data.extend(filtered_data)
                log.info(f"Retrieved {len(filtered_data)} rows (offset {offset})")

                total_rows = int(data["response"].get("total", 0))
                offset += length

                if offset >= total_rows:
                    break

        except requests.exceptions.RequestException as e:
            log.error(f"API connection failed: {str(e)}")
            raise
    
    duration = datetime.now() - start_time
    # ===== LOCAL STORAGE =====
    try:
        if all_data:
            df = pd.DataFrame(all_data)
            log.info("Ejemplo de fila:")
            print(df.head(1).to_string(index=False))
            df.to_parquet(output_file, index=False)
            log.info(f"Saved {len(df)} records as Parquet in {duration.total_seconds():.2f}s")
        else:
            log.warning("No hay datos para guardar en Parquet")
    except Exception as e:
        log.error(f"Failed to save data to {output_file}: {e}")
        raise
    
    """# ===== HDFS STORAGE =====
    try:
        log.info(f"Transferring to HDFS: {hdfs_dir}")
        hdfs_manager.copy_from_local(output_file, hdfs_dir)
    except Exception as e:
        log.error(f"HDFS transfer failed: {str(e)}")
        raise"""
    

load_data_electricity("2019-01-01", "2024-03-31")