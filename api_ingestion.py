#----------------------------------------------------------#
#                    Imports & Config                      #
#----------------------------------------------------------#
from kafka import KafkaProducer
import requests
from dotenv import load_dotenv
import os
from datetime import datetime, UTC
import time
import json

load_dotenv()
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")


#----------------------------------------------------------#
#                   Fetch API Methods                      #
#----------------------------------------------------------#

def fetching_weather():
    """
    Fetch CURRENT weather from OpenWeather.
    We only care about current conditions (real-time).
    """
    lat = 38.6270 
    lon = -90.1994

    url = (
        f"https://api.openweathermap.org/data/3.0/onecall?"
        f"lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}"
        f"&units=metric&exclude=minutely,hourly,daily,alerts"
    )

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Weather API failed: {response.status_code}")

    data = response.json()
    current = data.get("current", {})

    rain_data = current.get("rain", {})
    # Weather JSON record
    return {
        "source": "weather",
        "timestamp": current.get("dt"),
        "weather": current.get("weather", [{}])[0].get("main"),
        "visibility": current.get("visibility"),
        "rain": rain_data.get("1h", 0.0) if rain_data else 0.0,
        "ingestion_time": datetime.now(UTC).isoformat()
    }


ICON_CATEGORY = {
    0: "Unknown",
    1: "Accident",
    2: "Fog",
    3: "Dangerous Conditions",
    4: "Rain",
    5: "Ice",
    6: "Lane Closure",
    7: "Road Closure",
    8: "Road Closed",
    9: "Road Works",
    10: "Wind",
    11: "Flooding",
    14: "Broken Down Vehicle",
}

MAGNITUDE_LABELS = {
    0: "Unknown",
    1: "Minor",
    2: "Moderate",
    3: "Major",
    4: "Undefined",
}

def fetching_incidents():
    """
    Fetch traffic incidents from TomTom API using a configurable bounding box.
    """

    # Saint Louis center coordinates
    lat = 38.6270 
    lon = -90.1994
    # Controls how large the search area is
    bbox_offset = 0.5  # increase for more incidents (max is 0.5 based on testing)

    min_lon = lon - bbox_offset
    max_lon = lon + bbox_offset
    min_lat = lat - bbox_offset
    max_lat = lat + bbox_offset

    bbox = f"{min_lon},{min_lat},{max_lon},{max_lat}"

    url = (
        f"https://api.tomtom.com/traffic/services/5/incidentDetails"
        f"?key={TOMTOM_API_KEY}"
        f"&bbox={bbox}"
        f"&fields={{incidents{{type,geometry{{type,coordinates}},properties{{id,iconCategory,magnitudeOfDelay,startTime,endTime}}}}}}"
        f"&language=en-US"
        f"&timeValidityFilter=present"
    )

    response = requests.get(url)

    print("TomTom response:", response.text)

    if response.status_code != 200:
        raise Exception(f"TomTom API failed: {response.status_code}")
    

    data = response.json()
    cleaned = []

    for item in data.get("incidents", []):
        props = item.get("properties", {})
        geometry = item.get("geometry", {})

        # if props.get("iconCategory") not in [1,6,14]:
        #     continue
        if props.get("iconCategory") in [2,4,5,10,11]:
            continue
        coords = geometry.get("coordinates", [])
        if not coords:
            continue

        # Traffic JSON record
        icn_cat = props.get("iconCategory")
        mag = props.get("magnitudeOfDelay")
        cleaned.append({
            "source": "traffic",
            "id": props.get("id"),
            "type": ICON_CATEGORY.get(icn_cat, f"Category {icn_cat}"),
            "severity": MAGNITUDE_LABELS.get(mag, "Unknown"),
            "start_time": props.get("startTime"),
            "ingestion_time": datetime.now(UTC).isoformat()
        })

    return cleaned


#----------------------------------------------------------#
#                   Kafka Producer Setup                   #
#----------------------------------------------------------#

def on_send_success(record_metadata):
    print(f"Sent to {record_metadata.topic} | offset {record_metadata.offset}")

def on_send_error(excp):
    print("Kafka send error:", excp)


#----------------------------------------------------------#
#                        Main Method                       #
#----------------------------------------------------------#

if __name__ == "__main__":

    # kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    while True:
        try:
            # weather
            weather = fetching_weather()
            producer.send("weather_topic", weather) \
                .add_callback(on_send_success) \
                .add_errback(on_send_error)

            # traffic
            incidents = fetching_incidents()
            for incident in incidents:
                producer.send("traffic_topic", incident) \
                    .add_callback(on_send_success) \
                    .add_errback(on_send_error)

            producer.flush()

            print(f"Sent {len(incidents)} incidents + 1 weather record")

        except Exception as e:
            print("Error:", e)
            time.sleep(10)

        time.sleep(60) # every 60 seconds call APIs