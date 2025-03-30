from dags.utils.landing.class_types import TrafficAccId
from dags.utils.hdfs_utils import HDFSManager
from datetime import datetime
from sodapy import Socrata # type: ignore
from pathlib import Path
import subprocess
import logging
import json
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO, # minimum logging level
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S')

# Create a module-specific logger
log = logging.getLogger(__name__)

MODULE_VARS = {
    'domain': 'API_DOMAIN_TRAFFIC_ACC',
    'dataset': 'SOC_DB_ID_TRAFFIC_ACC',
    'token': 'APP_TOKEN_TRAFFIC_ACC'
}


def load_data_traffic_acc(hdfs_manager: HDFSManager,
                      start_date: str,
                      end_date: str):
    """
    Loads data from all three selected stations
    """
    try:
        # Process each area one by one
        load_area_data(TrafficAccId.LONG_BEACH, start_date, end_date, hdfs_manager)
        load_area_data(TrafficAccId.DOWNTOWN, start_date, end_date, hdfs_manager)
        load_area_data(TrafficAccId.RESEDA, start_date, end_date, hdfs_manager)
    finally:
        # Clean up temporary files after ALL areas have been processed
        subprocess.run(["rm", "-rf", '/tmp/traffic_acc/'], check=True)


def load_area_data(area: TrafficAccId, 
                          start_date: str, 
                          end_date: str, 
                          hdfs_manager: HDFSManager) -> None:
    """
    HDFS storage of LAcity traffic accidents data (JSON) using SoQL queries to a speficic area, from start date to end date.
    """
    # ===== DATE VALIDATION =====
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        if start_dt > end_dt:
            raise ValueError("Start date after end date")
    except ValueError as e:
        log.error(f"Date validation failed: {str(e)}")
        raise

    # ==== PATH CONFIGURATION ====
    tmp_dir = Path(f'/tmp/traffic_acc/{area.name}')
    hdfs_dir = "/data/landing/traffic_acc"

    # Ensure the directory exists
    tmp_dir.mkdir(parents=True, exist_ok=True)
    output_file = tmp_dir / f"{start_date}_{end_date}.json"

    # ===== API CONNECTION =====
    client = Socrata(
        domain=os.getenv(MODULE_VARS['domain']),
        app_token=os.getenv(MODULE_VARS['token']))

    # ===== SOCRATA QUERY =====
    query = (
        f"area_name = '{area.value}' AND "
        f"date_occ BETWEEN '{start_date}' AND '{end_date}'")

    # ===== DATA EXTRACTION =====
    try:
        log.info(f"Querying traffic data for {area.name}")
        start_time = datetime.now()
        results = client.get(
            os.getenv(MODULE_VARS['dataset']),
            where=query,
            limit=100000)
        duration = datetime.now() - start_time

        if not results:
            log.warning(f"No data found for {area.name} ({start_date} to {end_date})")
            return

        # ===== LOCAL STORAGE =====
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        log.info(f"Retrieved {len(results)} records in {duration.total_seconds():.2f}s")

        try:
            # ===== HDFS STORAGE =====
            log.info(f"Transferring to HDFS: {hdfs_dir}")
            hdfs_manager.copy_from_local(str(output_file), f"{hdfs_dir}/{area.name}")
        except Exception as e:
            log.error(f"HDFS transfer failed: {str(e)}")
            raise

    except Exception as e:
        log.error(f"API operation failed: {str(e)}")
        raise
    finally:
        client.close()