from kafka import KafkaProducer
import requests
from dotenv import load_dotenv
import os
from datetime import datetime
import time

import json

load_dotenv()

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
HERE_API_KEY = os.getenv("HERE_API_KEY")


#----------------------------------------------------------#
#                     Fetch API Methods                    #
#----------------------------------------------------------#


def fetching_weather():
    lat = 38.6270 
    lon = -90.1994

    url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
    response = requests.get(url)
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
        "ingestion_time": datetime.now().isoformat()
    }


def fetching_incidents():
    bbox = "-90.3,38.5,-90.0,38.8"

    url = f"https://data.traffic.hereapi.com/v7/incidents?in=bbox:{bbox}&locationReferencing=shape&apiKey={HERE_API_KEY}"
    response = requests.get(url)
    data = response.json()

    cleaned = []

    for item in data.get("results", []):
        details = item.get("incidentDetails", {})
        location = item.get("location", {})

        try:
            point = location["shape"]["links"][0]["points"][0]
            lat = point["lat"]
            lon = point["lng"]
        except:
            lat, lon = None, None

        cleaned.append({
            "source": "traffic",
            "id": details.get("id"),
            "type": details.get("type"),
            "severity": details.get("criticality"),
            "description": details.get("description", {}).get("value"),
            "start_time": details.get("startTime"),
            "lat": lat,
            "lon": lon,
            "ingestion_time": datetime.now().isoformat()
        })

    return cleaned


# ---------- Kafka Producer ----------
def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)
 
def on_send_error(excp):
    print("Error sending message:", excp)


#----------------------------------------------------------#
#                         Main Method                      #
#----------------------------------------------------------#

if __name__ == "__main__":
    producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    while True:
        try:
            weather = fetching_weather()
            producer.send("weather_topic", weather)

            incidents = fetching_incidents()
            for incident in incidents:
                producer.send("traffic_topic", incident)

            print(f"Sent {len(incidents)} incidents and 1 weather record")

        except Exception as e:
            print("Error:", e)

        time.sleep(60)
