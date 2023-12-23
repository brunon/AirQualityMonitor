import time
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
TIMEZONE = "EST"

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
last_publish_weather_timestamp = None
last_publish_forecast_timestamp = None
influxdb = InfluxWriter("/etc/enviro/config.yaml", "source")


def download_weather_xml():
    """
    download Environment Canada weather XML file
    """
    with urllib.request.urlopen(WEATHER_URL) as f:
        tree = ET.parse(f)

    return tree.getroot()


def main():
    global last_publish_weather_timestamp, last_publish_forecast_timestamp
    root = download_weather_xml()

    weather_timestamp = datetime.strptime(root.find(f"./currentConditions/dateTime[@zone='{TIMEZONE}']/timeStamp").text, WEATHER_TIMESTAMP)
    current_temperature = float(root.find("./currentConditions/temperature").text)
    pressure = float(root.find("./currentConditions/pressure").text)
    humidity = float(root.find("./currentConditions/relativeHumidity").text)
    location = root.find("./currentConditions/station").get("code")
    if last_publish_weather_timestamp is None or weather_timestamp > last_publish_weather_timestamp:
        print(f"Posting current conditions: temp {current_temperature}C humidity {humidity}% pressure {pressure}kPa", flush=True)
        influxdb.publish(f"ec-{location}", {
            "temperature": current_temperature,
            "humidity": humidity,
            "pressure": pressure,
            },
            bucket="weather",
            timestamp=int(weather_timestamp.timestamp()))
        last_publish_weather_timestamp = weather_timestamp

    forecast_ts = datetime.strptime(root.find(f"./forecastGroup/dateTime[@zone='{TIMEZONE}']/timeStamp").text, WEATHER_TIMESTAMP)
    current_weekday = date.today().isoweekday()
    if last_publish_forecast_timestamp is None or forecast_ts > last_publish_forecast_timestamp:
        for forecast in root.findall("./forecastGroup/forecast"):

            name = forecast.find("./period").text
            weekday = name.split(' ')[0]
            isoweekday = ISOWEEK_MAPPING.get(weekday)
            is_night = name.endswith("night")
            days_diff = ((isoweekday - current_weekday) + 7) % 7
            if days_diff == 0: continue

            future_ts = (datetime.now() + timedelta(days=days_diff)).replace(minute=0, second=0, microsecond=0)
            if is_night:
                future_ts = (future_ts + timedelta(days=1)).replace(hour=0) # midnight on next day
            else:
                future_ts = future_ts.replace(hour=12) # noon
            
            temp = float(forecast.find("./temperatures/temperature").text)
            print(f"Forecast for {future_ts} is {temp}C", flush=True)

            influxdb.publish(f"ec-{location}", {
                f"forecast-{days_diff}": temp,
                },
                bucket="weather",
                timestamp=int(future_ts.timestamp()))
        last_publish_forecast_timestamp = forecast_ts


while True:
    try:
        main()
    except Exception as ex:
        print("Error during execution: " + ex)

    time.sleep(10 * 60) # wait 10 minutes and try again

