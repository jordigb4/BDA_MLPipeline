from enum import Enum

"""
Identifiers of the stations involved in the analysis
Long Beach, Downtown LA, and Reseda are chosen due to its geographical location
"""

# Enum class to store the identifiers of the weather stations
class WeatherStationId(Enum):
    LONG_BEACH = "USW00023129"
    DOWNTOWN = "USW00093134"
    RESEDA = "USC00049785"

# Enum class to store the identifiers of the air stations
class AirStationId(Enum):
    LONG_BEACH = "8679"
    DOWNTOWN = "7936"
    RESEDA = "2138"

# Enum class to store the identifiers of the reporting traffic accident areas
class TrafficAccId(Enum):
    LONG_BEACH = "Harbor"
    DOWNTOWN = 'Central'
    RESEDA = 'West Valley'
