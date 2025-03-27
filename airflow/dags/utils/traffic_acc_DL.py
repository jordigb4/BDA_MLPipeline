import os
import json
import logging
from datetime import datetime
from sodapy import Socrata

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

MODULE_VARS = {
    'domain': 'API_DOMAIN_TRAFFIC_ACC',
    'dataset': 'SOC_DB_ID_TRAFFIC_ACC',
    'token': 'APP_TOKEN_TRAFFIC_ACC'
}


def load_traffic_acc_data(area: str, 
                          start_date: str, 
                          end_date: str, 
                          output_dir: str = '/data/raw/traffic') -> None:
    """
    Download LAcity data from Los Angeles Police Department using SoQL queries to a speficic area, from start date to end date.
    """
    try:
        # Validate date format and order
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            if start > end:
                log.error("Start date cannot be after end date")
                raise

        except ValueError as e:
            log.error(f"Invalid date format: {str(e)}")
            raise

        # Setup API client
        client = Socrata(
            domain=os.getenv(MODULE_VARS['domain']),
            app_token=os.getenv(MODULE_VARS['token'])
        )

        # Build query
        query = (
            f"area_name = '{area}' AND "
            f"date_occ BETWEEN '{start_date}' AND '{end_date}'"
        )

        # Fetch raw data
        results = client.get(
            os.getenv(MODULE_VARS['dataset']),
            where=query,
            limit=100000
        )

        if not results:
            log.warning(f"No data found for {area} ({start_date} to {end_date})")

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Generate filename
        filename = f"{area}_{start_date}_{end_date}_raw.json"
        filepath = os.path.join(output_dir, filename)

        # Write raw JSON to file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2) # indent=2 for human-readable formatting (remove for compact JSON) 

        log.info(f"Successfully saved {len(results)} records to {filepath}")
    
    except Exception as e:
        log.error(f"Failed to process data: {str(e)}")
   
    finally:
        if 'client' in locals():
            client.close()