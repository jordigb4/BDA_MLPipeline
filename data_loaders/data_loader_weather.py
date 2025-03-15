import logging
import os
import subprocess

from dotenv import load_dotenv

from class_types import WeatherStationId

load_dotenv()

def load_data_weather(STATION_ID: WeatherStationId):

    SYNC_COMMAND_WEATHER_TEMPLATE = os.getenv('SYNC_COMMAND_WEATHER')

    command = SYNC_COMMAND_WEATHER_TEMPLATE.format(STATION_ID=STATION_ID)
    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    logging.info(result)


load_data_weather("USC00049785")