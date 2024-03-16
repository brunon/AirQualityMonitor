import os
import time
import json
import argparse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from influx import InfluxWriter

PROVINCE = "QC"
LANGUAGE = "e"
LOCATION_CODE = "s0000551"
TODAY = datetime.now().strftime("%Y%m%d")
WEATHER_URL = f"https://hpfx.collab.science.gc.ca/{TODAY}/WXO-DD/citypage_weather/xml/{PROVINCE}/{LOCATION_CODE}_{LANGUAGE}.xml"
WEATHER_TIMESTAMP = "%Y%m%d%H%M%S" # e.g. 20231222154500
TIMEZONES = list(time.tzname)

ISOWEEK_MAPPING = {
        "Monday": 1,
        "Tuesday": 2,
        "Wednesday": 3,
        "Thursday": 4,
        "Friday": 5,
        "Saturday": 6,
        "Sunday": 7
        }

# connection to local InfluxDB
influxdb = InfluxWriter("/etc/enviro/config.yaml", "source")

# setup CLI argument parser
parser = argparse.ArgumentParser()
parser.add_argument('--nfs', required=True, dest='nfs_dir', help="NFS Directory to get status file from")
args = parser.parse_args()
nfs_dir = args.nfs_dir

json_file = f"{nfs_dir}/weather.json"
if os.path.exists(json_file):
    with open(f"{nfs_dir}/weather.json", "r") as f:
        j = json.load(f)
        last_publish_weather_timestamp = datetime.strptime(j.get("last_publish_weather"), WEATHER_TIMESTAMP)
        last_publish_forecast_timestamp = datetime.strptime(j.get("last_publish_forecast"), WEATHER_TIMESTAMP)
else:
    last_publish_weather_timestamp = None
    last_publish_forecast_timestamp = None


def download_weather_xml():
    """
    download Environment Canada weather XML file
    """
    with urllib.request.urlopen(WEATHER_URL) as f:
        tree = ET.parse(f)

    return tree.getroot()


def get_tz_timestamp(root, xpath_tmpl):
    for tz in TIMEZONES:
        xp = xpath_tmpl.format(tz=tz)
        timestamp = root.find(xpath_tmpl.format(tz=tz))
        if timestamp is not None:
            yield timestamp


def get_and_publish_weather():
    global last_publish_weather_timestamp, last_publish_forecast_timestamp

    root = download_weather_xml()
    now = datetime.now()

    current_conditions = next(get_tz_timestamp(root, "./currentConditions/dateTime[@zone='{tz}']/timeStamp")).text
    weather_timestamp = datetime.strptime(current_conditions, WEATHER_TIMESTAMP)
    current_temperature = float(root.find("./currentConditions/temperature").text)
    pressure = float(root.find("./currentConditions/pressure").text)
    humidity = float(root.find("./currentConditions/relativeHumidity").text)
    location = root.find("./currentConditions/station").get("code")
    if last_publish_weather_timestamp is None or weather_timestamp > last_publish_weather_timestamp:
        print(f"{now} Posting current conditions: temp {current_temperature}C humidity {humidity}% pressure {pressure}kPa", flush=True)
        influxdb.publish(f"ec-{location}", {
            "temperature": current_temperature,
            "humidity": humidity,
            "pressure": pressure * 10.0,
            },
            bucket="weather",
            timestamp=int(weather_timestamp.timestamp()))
        last_publish_weather_timestamp = weather_timestamp
    else:
        print(f"Weather data ({weather_timestamp}) same as last publish ({last_publish_weather_timestamp}), skipping")

    forecast_ts = datetime.strptime(next(get_tz_timestamp(root, "./forecastGroup/dateTime[@zone='{tz}']/timeStamp")).text, WEATHER_TIMESTAMP)
    current_weekday = date.today().isoweekday()
    if last_publish_forecast_timestamp is None or forecast_ts > last_publish_forecast_timestamp:
        for forecast in root.findall("./forecastGroup/forecast"):

            name = forecast.find("./period").text
            weekday = name.split(' ')[0]
            isoweekday = ISOWEEK_MAPPING.get(weekday)
            is_night = name.endswith("night")
            days_diff = ((isoweekday - current_weekday) + 7) % 7
            if days_diff == 0: continue

            future_ts = (now + timedelta(days=days_diff)).replace(minute=0, second=0, microsecond=0)
            if is_night:
                future_ts = (future_ts + timedelta(days=1)).replace(hour=0) # midnight on next day
            else:
                future_ts = future_ts.replace(hour=12) # noon
            
            temp = float(forecast.find("./temperatures/temperature").text)
            print(f"{now} Forecast for {future_ts} is {temp}C", flush=True)

            influxdb.publish(f"ec-{location}", {
                f"forecast-{days_diff}": temp,
                },
                bucket="weather",
                timestamp=int(future_ts.timestamp()))
        last_publish_forecast_timestamp = forecast_ts
    else:
        print(f"Forecast data ({forecast_ts}) same as last publish ({last_publish_forecast_timestamp}), skipping")


# get data
get_and_publish_weather()

# write timestamps to nfs
with open(f"{nfs_dir}/weather.json", "w") as f:
    js = json.dumps({
        "last_publish_weather": last_publish_weather_timestamp.strftime(WEATHER_TIMESTAMP),
        "last_publish_forecast": last_publish_forecast_timestamp.strftime(WEATHER_TIMESTAMP)
        })
    f.write(js)

