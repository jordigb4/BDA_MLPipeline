import os
import requests
import json
import logging
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

def load_data_electricity(start_date: str, end_date: str, output_dir: str = './data'):
    api_key = os.getenv('API_KEY_ELECTRICITY')
    base_url = "https://api.eia.gov/v2/electricity/rto/daily-region-data/data/"

    output_file = os.path.join(output_dir, f"electricity_data_{start_date}_{end_date}.json")
    all_data = []

    offset = 0
    length = 5000  # Tamaño máximo permitido por la API

    try:
        os.makedirs(output_dir, exist_ok=True)
    except OSError as e:
        logger.error(f"Failed to create output directory {output_dir}: {e}")
        raise

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

        response = requests.get(base_url, params=params)

        if response.status_code == 200:
            data = response.json()

            filtered_data = []
            for entry in data.get("response", {}).get("data", []):
                if entry.get("type-name") == "Demand" and entry.get("timezone") == "Pacific":
                    entry.pop("type-name", None)
                    entry.pop("timezone-description", None)
                    filtered_data.append(entry)

            all_data.extend(filtered_data)
            logger.info(f"Downloaded {len(filtered_data)} rows (offset {offset})")

            total_rows = int(data["response"].get("total", 0))
            offset += length

            if offset >= total_rows:
                break
        else:
            logger.error(f"API request failed: {response.status_code}, {response.text}")
            break

    try:
        with open(output_file, "w") as file:
            json.dump(all_data, file, indent=4)
        logger.info(f"Successfully saved electricity data to {output_file}")
    except OSError as e:
        logger.error(f"Failed to save data to {output_file}: {e}")
        raise

if __name__ == "__main__":
    try:
        load_data_electricity("2019-01-01", "2024-03-31")
    except Exception as e:
        logger.critical(f"Critical failure in electricity data pipeline: {e}")
        raise SystemExit(1) from e