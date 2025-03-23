import logging
import os
import subprocess
from pathlib import Path
from typing import Optional

from .class_types import WeatherStationId


logger = logging.getLogger(__name__)

def load_data_weather(hdfs_manager):

    # Load data from all three stations
    load_data_from_station(WeatherStationId.LONG_BEACH,hdfs_manager)
    load_data_from_station(WeatherStationId.DOWNTOWN,hdfs_manager)
    load_data_from_station(WeatherStationId.RESEDA,hdfs_manager)

    subprocess.run(["rm", "-rf", '/tmp/weather/'], check=True)
def load_data_from_station(
        station_id: WeatherStationId,
        hdfs_manager,
) -> None:
    """
    Load weather data for a specific station

    Args:
        station_id: Weather station identifier from the WeatherStationId enum
    """
    # Get command
    weather_sync_command_template = os.getenv("SYNC_COMMAND_WEATHER")

    # Directories names
    tmp_dir = '/tmp/weather/' + station_id.name
    hdfs_dir = f"/data/landing/weather"

    # Create directories
    tmp_path = Path(tmp_dir)
    tmp_path.mkdir(parents=True, exist_ok=True)

    # Format ingestion command
    formatted_command = weather_sync_command_template.format(
        STATION_ID=station_id.value,
        OUTPUT_DIR=tmp_dir
    )

    logger.info(
        f"Initiating weather data download for station {station_id.name} "
        f"Using command: {formatted_command}"
    )

    try:
        # Execute ingestion command
        result = subprocess.run(
            formatted_command.split(),
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        if result.stderr:
            logger.info(f"Command stderr: {result.stderr}")

        # Copy contents to hdfs
        hdfs_manager.copy_from_local(tmp_dir,hdfs_dir)

    except Exception as e:
        logger.error(f"Unexpected error during data download: {e}")
        raise

    logger.info(
        f"Successfully downloaded weather data for station {station_id.name} "
    )
