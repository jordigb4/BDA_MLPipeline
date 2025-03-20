import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()  

def load_data_electricity(start_date: str, end_date: str, output_dir: str = './data'):
    api_key = os.getenv('API_KEY_ELECTRICITY')  
    
    base_url = "https://api.eia.gov/v2/electricity/rto/daily-region-data/data/"
    params = {
        "api_key": api_key,
        "frequency": "daily",
        "data[0]": "value",
        "facets[respondent][]": "LDWP",  
        "start": start_date,
        "end": end_date,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000
    }
    
  
    
    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, f"electricity_data_{start_date}_{end_date}.json")
        with open(file_path, "w") as file:
            json.dump(response.json(), file, indent=4)
        print(f"Datos guardados en {file_path}")
    else:
        print(f"Error al obtener datos: {response.status_code}, {response.text}")


load_data_electricity("2019-01-01T00", "2024-03-31T00")