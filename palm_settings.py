# version 2023.09.29
"""
Settings file for use with palm.py: Compatible with v0.9, v0.10, v1.0.x and v1.1.x
2023.09.29: added Shelly power monitor
"""
######
# Do not edit this class definition, it is used to share global variables between components of the PALM system
class pg:
    """PALM global variable definitions. Used by palm_utils and project-specific wrappers"""

    test_mode: bool = False
    debug_mode: bool = False
    once_mode: bool = False
    long_t_now: str = ""
    month: str = ""
    t_now: str = ""
    t_now_mins: int = 0
    loop_counter: int = 0  # 1 minute minor frame. "0" = initialise
    pvo_tstamp: int = 0  # Records value of loop_counter when PV data last written
    palm_version: str = ""
######
# Edit from this point onwards

# SolCast PV forecast generator. Two forecasts are obtained, one per array
class Solcast:
    enable = True
    key = "xxxx"
    url_se = "https://api.solcast.com.au/rooftop_sites/xxxx"
    url_sw = "https://api.solcast.com.au/rooftop_sites/xxxx"
    cmd = "/forecasts?format=json"
    weight = 35  # Confidence factor for forecast (range 10 to 90)

class PVData:
    PwrThreshold = 30  # Sets power threshold for virtual sunset (lighting up time)

 # PVOutput.org data logging/analysis service
class PVOutput:
    enable = True
    url= "https://pvoutput.org/service/r2/"
    key = "xxxx"
    sid = "xxxx"

# API for obtaining current UK carbon intensity of electricity generation
class CarbonIntensity:
    enable = True
    url = "https://api.carbonintensity.org.uk/regional/intensity/"
    RegionID = "/fw24h/regionid/15"
    Threshold = 250  # For Used to trigger afternoon battery charge
    Multiplier = 1.5  # For afternoon battery charge

# Weather data
class OpenWeatherMap:
    enable = True
    url = "https://api.openweathermap.org/data/2.5/"
    url_weather = "https://api.openweathermap.org/data/2.5/weather?"
    url_forecast = "https://api.openweathermap.org/data/2.5/forecast?"
    city_id = "xxxx"
    payload = {
    "lat"   : 51.05,
    "lon"   : -0.985,
    "appid" : "xxxx",
    "exclude" : "daily",
    "mode"  : 'JSON'
    }

# User settings for GivEnergy inverter API
class GE:
    enable = True
    url = "https://api.givenergy.cloud/v1/inverter/CEXXXX/"
    key="XXXX"

    # Disable SoC calculation in the winter months as consumption >> generation
    winter = ["01", "02", "11", "12"]

    # Throttle SoC calculation in shoulder months as consumption can vary with heating coming on
    shoulder = ["03", "04", "09", "10"]

    # Lower limit for state of charge (summertime)
    min_soc_target = 20

    # Higher SoC limit for shoulder months
    max_soc_target = 60

    # Battery reserve for power cuts (minmum of 4%)
    batt_reserve = 4

    # Nominal battery capacity
    batt_capacity = 10.4

    # Proportion of battery that's usable
    batt_utilisation = 0.85

    batt_max_charge = batt_capacity * batt_utilisation

    # Inverter charge/discharge rate
    charge_rate = 3

    # Default data for base load. Overwritten by actual data if available
    base_load = [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.2, \
                 0.2, 0.2, 0.2, 0.3, 0.2, 0.2, 0.1, 0.3, 0.3, 0.2, 0.3, 0.8, 0.6, 0.3, 0.3, 0.2, \
                 0.2, 0.2, 0.2, 0.6, 0.6, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1]

    # Load history is a weighted average of actual load from previous days.
    # Uncomment required settings or make your own using positive integers only. Examples:
    # Most recent day only
    # load_hist_weight = [1]
    # 3-day average
    load_hist_weight = [1, 1, 1]
    # 7-day average
    # load_hist_weight = [1, 1, 1, 1, 1, 1, 1]
    # Same day last week - useful if, say, Monday is always wash-day
    # load_hist_weight = [0, 0, 0, 0, 0, 0, 1]
    # Weighted average (a more extreme example)
    # load_hist_weight = [4, 2, 2, 1, 1, 1, 1]

    # Start time for Overnight Charge. If "", disables writing this inverter register
    # Be kind to the grid and set your own value that isn't "on the hour/half-hour" to stagger demand
    start_time = "00:37"

    # End time for Overnight Charge period. If "", disables writing this inverter register
    end_time = "04:30"

    # Delayed winter end time saves battery for morning peak. If "", disables writing this inverter register
    end_time_winter = "06:30"    

    # Winter afternoon boost start and end times
    boost_start = "13:03"
    boost_finish = "16:44"

class Shelly:
    em0_url = "http://192.168.1.21/emeter/0"


# MiHome devices are used to activate various loads
class MiHome:
    enable = True
    url = "https://mihome4u.co.uk/api/v1/subdevices/"
    UserID = "XXXX"
    key = "XXXX"

# This section defines the various loads and operating parameters
LOAD_CONFIG = {
    "LoadPriorityOrder": [
        "Lights1",
        "Lights2",
        "Lights3",
        "Lights4",
        "Load1",
        "Load2"
    ],
    "Load1": {
        "DeviceName"    :       "Towel Rails",
        "DeviceID"      :       "182182",
        "EarlyStart"    :       "10:00",
        "LateStart"     :       "14:00",
        "FinishTime"    :       "16:00",
        "MinOnTime"     :        5,
        "MinDailyTarget":        0,
        "MaxDailyTarget":        60,
        "MaxCO2"        :        200,
        "MaxTemp"       :        20,
        "PwrLoad"       :        800,
        "PwrStart"      :        500,
        "Hysteresis"    :        250
    },
    "Load2": {
        "DeviceName"    :       "Battery Charger",
        "DeviceID"      :       "249595",
        "EarlyStart"    :       "09:00",
        "LateStart"     :       "15:00",
        "FinishTime"    :       "16:00",
        "MinOnTime"     :        5,
        "MinDailyTarget":        15,
        "MaxDailyTarget":        120,
        "MaxCO2"        :        300,
        "MaxTemp"       :        25,
        "PwrLoad"       :        40,
        "PwrStart"      :        30,
        "Hysteresis"    :        50
    },
    "Lights1": {
        "DeviceName"    :       "Lights - Hall",
        "DeviceID"      :       "176890",
        "EarlyStart"    :       "VSunset",
        "LateStart"     :       "VSunset",
        "FinishTime"    :       "22:00",
        "MinOnTime"     :        10,
        "MinDailyTarget":        480,
        "MaxDailyTarget":        480,
        "MaxCO2"        :        500,
        "MaxTemp"       :        50,
        "PwrLoad"       :        10,
        "PwrStart"      :        0,
        "Hysteresis"    :        0
    },
    "Lights2": {
        "DeviceName"    :       "Lights - Standard Lamp",
        "DeviceID"      :       "176922",
        "EarlyStart"    :       "VSunset",
        "LateStart"     :       "VSunset",
        "FinishTime"    :       "22:45",
        "MinOnTime"     :        10,
        "MinDailyTarget":        480,
        "MaxDailyTarget":        480,
        "MaxCO2"        :        500,
        "MaxTemp"       :        50,
        "PwrLoad"       :        60,
        "PwrStart"      :        0,
        "Hysteresis"    :        0
    },
    "Lights3": {
        "DeviceName"    :        "Lights - Lounge Table Lamp",
        "DeviceID"      :        "221403",
        "EarlyStart"    :        "VSunset",
        "LateStart"     :        "VSunset",
        "FinishTime"    :        "23:59",
        "MinOnTime"     :        10,
        "MinDailyTarget":        480,
        "MaxDailyTarget":        480,
        "MaxCO2"        :        500,
        "MaxTemp"       :        50,
        "PwrLoad"       :        10,
        "PwrStart"      :        0,
        "Hysteresis"    :        0
    },
    "Lights4": {
        "DeviceName"    :        "Christmas Lights",
        "DeviceID"      :        "234342",
        "EarlyStart"    :        "VSunset",
        "LateStart"     :        "VSunset",
        "FinishTime"    :        "22:35",
        "MinOnTime"     :        10,
        "MinDailyTarget":        480,
        "MaxDailyTarget":        480,
        "MaxCO2"        :        500,
        "MaxTemp"       :        50,
        "PwrLoad"       :        10,
        "PwrStart"      :        0,
        "Hysteresis"    :        0
    },
    "LEAF": {
        "DeviceName"    :       "LEAF",
        "DeviceID"      :       "177842",
        "EarlyStart"    :       "Sunrise",
        "LateStart"     :       "23:59",
        "MinCO2"        :        90,
        "GenRatio"      :        10,
        "PwrLoad"       :        7000,
        "PwrStart"      :        1000
    }
}


GE_Command_list = {
    "data": [
        {
            "id": 17,
            "name": "Enable AC Charge Upper % Limit",
            "validation": "Value must be either true or false"
        },
        {
            "id": 24,
            "name": "Enable Eco Mode",
            "validation": "Value must be either true or false"
        },
        {
            "id": 28,
            "name": "AC Charge 2 Start Time",
            "validation": "Value format should be HH:mm. Use correct time range for hour and minutes"
        },
        {
            "id": 29,
            "name": "AC Charge 2 End Time",
            "validation": "Value format should be HH:mm. Use correct time range for hour and minutes"
        },
        {
            "id": 41,
            "name": "DC Discharge 2 Start Time",
            "validation": "Value format should be HH:mm. Use correct time range for hour and minutes"
        },
        {
            "id": 42,
            "name": "DC Discharge 2 End Time",
            "validation": "Value format should be HH:mm. Use correct time range for hour and minutes"
        },
        {
            "id": 47,
            "name": "Inverter Max Output Active Power Percent",
            "validation": "Value must be between 0 and 100"
        },
        {
            "id": 53,
            "name": "DC Discharge 1 Start Time",
            "validation": "Value format should be HH:mm. Use correct time range for hour and minutes"
        },
        {
            "id": 54,
            "name": "DC Discharge 1 End Time",
            "validation": "Value format should be HH:mm. Use correct time range for hour and minutes"
        },
        {
            "id": 56,
            "name": "Enable DC Discharge",
            "validation": "Value must be either true or false"
        },
        {
            "id": 64,
            "name": "AC Charge 1 Start Time",
            "validation": "Value format should be HH:mm. Use correct time range for hour and minutes"
        },
        {
            "id": 65,
            "name": "AC Charge 1 End Time",
            "validation": "Value format should be HH:mm. Use correct time range for hour and minutes"
        },
        {
            "id": 66,
            "name": "AC Charge Enable",
            "validation": "Value must be either true or false"
        },
        {
            "id": 71,
            "name": "Battery Reserve % Limit",
            "validation": "Value must be between 4 and 100"
        },
        {
            "id": 72,
            "name": "Battery Charge Power",
            "validation": "Value must be between 0 and 3000"
        },
        {
            "id": 73,
            "name": "Battery Discharge Power",
            "validation": "Value must be between 0 and 3000"
        },
        {
            "id": 75,
            "name": "Battery Cutoff % Limit",
            "validation": "Value must be between 4 and 100"
        },
        {
            "id": 77,
            "name": "AC Charge Upper % Limit",
            "validation": "Value must be between 0 and 100"
        },
        {
            "id": 83,
            "name": "Restart Inverter",
            "validation": "Value can only be 100"
        },
        {
            "id": 258,
            "name": "Force Off Grid",
            "validation": "Value must be either true or false"
        }
    ]
}
