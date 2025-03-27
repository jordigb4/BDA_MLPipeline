import os
import requests
import logging
import json

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


def load_data_electricity(start_date: str,
                          end_date: str,
                          output_dir: str = '/data/raw/') -> None:
    
    api_key = os.getenv('API_KEY_ELECTRICITY')  # Read API key from .env file
    
    base_url = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
    params = {
        "api_key": api_key,
        "frequency": "hourly",
        "data[0]": "value",
        "facets[respondent][]": "LDWP",  # Correct format
        "start": start_date,
        "end": end_date,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000
    }

    headers = {"Content-Type": "application/json"}
    
    response = requests.get(base_url, headers=headers, params=params)

    if response.status_code == 200:
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, f"electricity_data_{start_date}_{end_date}.json")
        with open(file_path, "w") as file:
            json.dump(response.json(), file, indent=4)
        print(f"Data stored at {file_path}")
    else:
        print(f"Data collecting error: {response.status_code}, {response.text}")
