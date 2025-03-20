import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()  

def load_data_electricity(start_date: str, end_date: str, output_dir: str = './data'):
    api_key = os.getenv('API_KEY_ELECTRICITY')
    base_url = "https://api.eia.gov/v2/electricity/rto/daily-region-data/data/"

    output_file = os.path.join(output_dir, f"electricity_data_{start_date}_{end_date}.json")
    all_data = []  

    offset = 0
    length = 5000  # Mida màxima perquè funcioni la api

    os.makedirs(output_dir, exist_ok=True)  

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

            filtered_data = [
                entry for entry in data.get("response", {}).get("data", [])
                if entry.get("type-name") == "Demand" and entry.get("timezone") == "Pacific"
            ]
            all_data.extend(filtered_data)

            print(f"Descargadas {len(filtered_data)} filas (offset {offset})")

            total_rows = int(data["response"].get("total", 0))
            offset += length  

            if offset >= total_rows:
                break  
        else:
            print(f"Error en la solicitud: {response.status_code}, {response.text}")
            break

  
    with open(output_file, "w") as file:
        json.dump(all_data, file, indent=4)

    print(f"Datos guardados en {output_file}")


load_data_electricity("2019-01-01", "2024-03-31")