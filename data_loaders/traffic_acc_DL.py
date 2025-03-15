import os
import json
import logging
from datetime import datetime
from sodapy import Socrata
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
)

ENV_VARS = {
    'domain': 'API_DOMAIN_TRAFFIC_ACC',
    'dataset': 'SOC_DB_ID_TRAFFIC_ACC',
    'token': 'APP_TOKEN_TRAFFIC_ACC'
}

def load_traffic_acc_data(area: str, start_date: str, end_date: str, output_dir: str = './data') -> str:
    """
    Download LAcity data from Los Angeles Police Department using SoQL queries to a speficic area, from start date to end date.
    """
    try:
        try:
            # Validate date format and order
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            if start > end:
                raise ValueError("Start date cannot be after end date")

        except ValueError as e:
            logging.error(f"Invalid date format: {str(e)}")
            raise

        # Setup API client
        client = Socrata(
            domain=os.getenv(ENV_VARS['domain']),
            app_token=os.getenv(ENV_VARS['token'])
        )

        # Build query
        query = (
            f"area_name = '{area}' AND "
            f"date_occ BETWEEN '{start_date}' AND '{end_date}'"
        )

        # Fetch raw data
        results = client.get(
            os.getenv(ENV_VARS['dataset']),
            where=query,
            limit=100000
        )

        if not results:
            logging.warning(f"No data found for {area} ({start_date} to {end_date})")

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Generate filename
        filename = f"{area}_{start_date}_{end_date}_raw.json"
        filepath = os.path.join(output_dir, filename)

        # Write raw JSON to file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2) # indent=2 for human-readable formatting (remove for compact JSON) 

        logging.info(f"Successfully saved {len(results)} records to {filepath}")
    
    except Exception as e:
        logging.error(f"Failed to process data: {str(e)}")
   
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    load_dotenv()
    
    result_file = load_traffic_acc_data(
        area='Central',
        start_date='2019-01-01',
        end_date='2019-01-31',
        output_dir='./traffic_data'
    )