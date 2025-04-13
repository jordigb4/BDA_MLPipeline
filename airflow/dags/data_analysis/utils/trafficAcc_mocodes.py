import pandas as pd
import re

mo_code_groups = {
    # Aid/Victim Related
    'Victim_Related': {
        '0101': 'Aid victim',
        '0102': 'Blind',
        '0103': 'Crippled',
        '0108': 'Infirm'
    },
    
    # Criminal Action Characteristics
    'Criminal_Action': {
        '0344': 'Orders vict to rear room',
        '0401': 'Force used',
        '0405': 'Bound',
        '0406': 'Brutal Assault',
        '0410': 'Covered victim\'s face',
        '0416': 'Hit-Hit w/ weapon',
        '0421': 'Threaten to kill',
        '0432': 'Intimidation',
        '0506': 'Forced to fondle suspect'
    },
    
    # Crime Context/Motivation
    'Crime_Context': {
        '0601': 'Business',
        '0602': 'Family',
        '0604': 'Reproductive Health Services',
        '0605': 'Traffic Accident related',
        '0906': 'Gangs',
        '0907': 'Narcotics',
        '0910': 'Public Transit',
        '0913': 'Victim knew Suspect',
        '0947': 'Estes Robbery'
    },
    
    # Suspect Activities
    'Suspect_Activities': {
        '1004': 'Assistant',
        '1026': 'Use phone/toilet'
    },
    
    # Victim Characteristics
    'Victim_Characteristics': {
        '1202': 'Aged/Disabled',
        '1208': 'Illegal Alien',
        '1218': 'Homeless/Transient',
        '1253': 'Bus Driver'
    },
    
    # Vehicle Involvement
    'Vehicle_Involvement': {
        '1300': 'Vehicle involved',
        '1303': 'Hid in rear seat',
        '1307': 'Breaks window',
        '1309': 'Susp uses vehicle'
    },
    
    # Evidence
    'Evidence': {
        '1402': 'Evidence Booked',
        '1407': 'Bullets/Casings',
        '1410': 'Gun Shot Residue',
        '1419': 'Firearm booked',
        '1501': 'Other MO'
    },
    
    # Relationships
    'Relationship': {
        '1803': 'Co-worker',
        '1816': 'Gang member',
        '1822': 'Stranger'
    },
    
    # Suspect Conditions/Characteristics
    'Suspect_Condition': {
        '2000': 'Domestic violence',
        '2001': 'On drugs',
        '2002': 'Intoxicated',
        '2004': 'Homeless/transient',
        '2005': 'Uses wheelchair',
        '2025': 'Bisexual',
        '2028': 'Repeat shoplifter',
        '2038': 'Restraining order',
        '2039': 'Costumed character',
        '2042': 'Short-term rental'
    },
    
    # Traffic Collision Types
    'Collision_Type': {
        '3001': 'Veh vs Non-collision',
        '3002': 'Officer Involved',
        '3003': 'Veh vs Ped',
        '3004': 'Veh vs Veh',
        '3005': 'Veh vs Veh other road',
        '3006': 'Veh vs Parked Veh',
        '3007': 'Veh vs Train',
        '3008': 'Veh vs Bike',
        '3009': 'Veh vs M/C',
        '3010': 'Veh vs Animal',
        '3011': 'Veh vs Fixed Object',
        '3012': 'Veh vs Other Object',
        '3013': 'M/C vs Veh',
        '3014': 'M/C vs Fixed Object',
        '3015': 'M/C vs Other',
        '3016': 'Bike vs Veh',
        '3017': 'Bike vs Train',
        '3018': 'Bike vs Other',
        '3019': 'Train vs Veh',
        '3020': 'Train vs Train',
        '3021': 'Train vs Bike',
        '3022': 'Train vs Ped',
        '3023': 'Train vs Fixed Object'
    },
    
    # Injury Levels
    'Injury_Level': {
        '3024': 'Severe Injury',
        '3025': 'Visible Injury',
        '3026': 'Complaint of Injury',
        '3027': 'Fatal Injury',
        '3028': 'Non Injury'
    },
    
    # Hit and Run / Property Classification
    'Hit_Run_Property': {
        '3029': 'Hit and Run Felony',
        '3030': 'Hit and Run Misd',
        '3032': 'Private Property - Yes',
        '3033': 'Private Property - No',
        '3034': 'City Property - Yes',
        '3035': 'City Property - No'
    },
    
    # Location Characteristics
    'Location_Type': {
        '3036': 'At Intersection - Yes',
        '3037': 'At Intersection - No'
    },
    
    # DUI/Race Related
    'DUI_Racing': {
        '3038': 'DUI Felony',
        '3039': 'DUI Misdemeanor',
        '3040': 'Street Racing',
        '3062': 'Bicyclist in Lane',
        '3063': 'Unknown 3063',
        '3064': 'Unknown 3064'
    },
    
    # Primary Collision Factor
    'Collision_Factor': {
        '3101': 'PCF In Narrative',
        '3102': 'Improper Driving',
        '3103': 'Other Than Driver',
        '3104': 'Unknown'
    },
    
    # Additional Traffic Classifications
    'Traffic_Class': {
        '3401': 'Type of Collision',
        '3501': 'Ped Actions',
        '3601': 'Special Information',
        '3602': 'Unlicensed motorist',
        '3603': 'Bike vs opened door',
        '3701': 'Movement Preceding',
        '3801': 'Sobriety'
    }
}


# Function to extract codes from mocodes string
def extract_codes(mocode_str):
    if pd.isna(mocode_str) or not isinstance(mocode_str, str):
        return []
    return re.findall(r'\d+', mocode_str)

# Function to find the first matching value for a specific group
def get_group_value(codes, group_dict):
    for code in codes:
        if code in group_dict:
            return group_dict[code]
    return "unknown"

def create_mocodes_features(df):

    # Create a copy of the dataframe to avoid modifying the original
    result_df = df.copy()
    
    # Extract codes for each row
    result_df['extracted_codes'] = result_df["mocodes"].apply(extract_codes)

    # Create a new column for each group
    for group_name, code_dict in mo_code_groups.items():
        result_df[group_name] = result_df['extracted_codes'].apply(
            lambda codes: get_group_value(codes, code_dict)
        )

    # Drop the temporary column
    result_df.drop('extracted_codes', axis=1, inplace=True)

    return result_df

