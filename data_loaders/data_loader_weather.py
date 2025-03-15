import logging
import os
import subprocess

from dotenv import load_dotenv

from class_types import WeatherStationId

load_dotenv()


def load_data_weather(station_id: WeatherStationId, output_dir: str = './data'):
    sync_command_weather_template = os.getenv('SYNC_COMMAND_WEATHER')

    command = sync_command_weather_template.format(STATION_ID=station_id, OUTPUT_DIR=output_dir)
    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    logging.info(result)


load_data_weather(WeatherStationId.LONG_BEACH)
