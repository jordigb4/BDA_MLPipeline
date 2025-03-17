import logging
import os
import subprocess
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from class_types import WeatherStationId

load_dotenv()

logger = logging.getLogger(__name__)


def load_data_weather(
        station_id: WeatherStationId,
        output_dir: str = "./data",
        timeout: Optional[int] = 300
) -> None:
    """
    Load weather data for a specific station using a configured command template.

    Args:
        station_id: Weather station identifier from the WeatherStationId enum
        output_dir: Output directory for downloaded data (default: './data')
        timeout: Timeout in seconds for the download command (default: 300)

    Raises:
        ValueError: If required environment variables are missing or invalid
        subprocess.CalledProcessError: If the underlying command fails
        OSError: For filesystem-related errors
    """
    # Validate environment configuration
    weather_sync_command_template = os.getenv("SYNC_COMMAND_WEATHER")
    if not weather_sync_command_template:
        raise ValueError("SYNC_COMMAND_WEATHER environment variable not set")

    # Create output directory if needed
    output_path = Path(output_dir)
    try:
        output_path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.error(f"Failed to create output directory {output_dir}: {e}")
        raise

    # Format command with safe parameter substitution
    formatted_command = weather_sync_command_template.format(
        STATION_ID=station_id.value,
        OUTPUT_DIR=output_dir
    )

    logger.info(
        f"Initiating weather data download for station {station_id.name} "
        f"using command: {formatted_command}"
    )

    try:
        # Execute command with security best practices
        result = subprocess.run(
            formatted_command.split(),
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
        logger.debug(f"Command stdout: {result.stdout}")

        if result.stderr:
            logger.info(f"Command stderr: {result.stderr}")

    except subprocess.CalledProcessError as e:
        logger.error(
            f"Command failed with code {e.returncode}\n"
            f"STDOUT: {e.stdout}\nSTDERR: {e.stderr}"
        )
        raise
    except subprocess.TimeoutExpired as e:
        logger.error(f"Command timed out after {timeout} seconds")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during data download: {e}")
        raise

    logger.info(
        f"Successfully downloaded weather data for station {station_id.name} "
        f"to {output_path.absolute()}"
    )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        load_data_weather(WeatherStationId.LONG_BEACH)
    except Exception as e:
        logger.critical(f"Critical failure in weather data pipeline: {e}")
        raise SystemExit(1) from e
