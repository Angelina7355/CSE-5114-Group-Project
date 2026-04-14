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
HERE_API_KEY = os.getenv("HERE_API_KEY")
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

    return {
        "source": "weather",
        "timestamp": current.get("dt"), 
        "temp": current.get("temp"),
        "humidity": current.get("humidity"),
        "wind_speed": current.get("wind_speed"),
        "weather": current.get("weather", [{}])[0].get("main"),
        "lat": lat,
        "lon": lon,
        "ingestion_time": datetime.now(UTC).isoformat()
    }

# below is the old HERE API code, which we switched to TomTom for better data quality and more flexible bounding box queries

# def fetching_incidents():
#     """
#     Fetch ACTIVE traffic incidents from HERE API.
#     API returns ALL active incidents (including old ones).
#     """
#     bbox = "-90.3,38.5,-90.0,38.8"

#     url = (
#         f"https://data.traffic.hereapi.com/v7/incidents?"
#         f"in=bbox:{bbox}&locationReferencing=shape&apiKey={HERE_API_KEY}"
#     )

#     response = requests.get(url)

#     if response.status_code != 200:
#         raise Exception(f"Traffic API failed: {response.status_code}")

#     data = response.json()
#     cleaned = []

#     for item in data.get("results", []):
#         details = item.get("incidentDetails", {})
#         location = item.get("location", {})

#         links = location.get("shape", {}).get("links", [])
#         if links and links[0].get("points"):
#             point = links[0]["points"][0]
#             lat = point.get("lat")
#             lon = point.get("lng")
#         else:
#             continue  # skip if no coordinates

#         cleaned.append({
#             "source": "traffic",
#             "id": details.get("id"),
#             "type": details.get("type"),
#             "severity": details.get("criticality"),
#             "description": details.get("description", {}).get("value"),
#             "start_time": details.get("startTime"),  # true event time
#             "lat": lat,
#             "lon": lon,
#             "ingestion_time": datetime.now(UTC).isoformat()
#         })

#     return cleaned

def fetching_incidents():
    """
    Fetch traffic incidents from TomTom API using a configurable bounding box.
    """

    # St louis center coordinates
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
        f"&fields={{incidents{{type,geometry{{type,coordinates}},properties{{id,iconCategory,startTime,endTime}}}}}}"
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

        coords = geometry.get("coordinates", [])
        if not coords:
            continue

        lon_val, lat_val = coords[0]

        cleaned.append({
            "source": "traffic",
            "id": props.get("id"),
            "type": props.get("iconCategory"),
            "severity": props.get("iconCategory"),
            "description": f"Category {props.get('iconCategory')}",
            "start_time": props.get("startTime"),
            "lat": lat_val,
            "lon": lon_val,
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