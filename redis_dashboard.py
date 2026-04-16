import json
import os
import pandas as pd
import redis
import streamlit as st
from streamlit_autorefresh import st_autorefresh

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    decode_responses=True
)

def load_from_redis():
    try:
        incidents_raw = redis_client.get("dashboard:recent_incidents")
        counts_raw = redis_client.get("dashboard:incident_counts")
        last_update = redis_client.get("dashboard:last_update_ts")
    except redis.exceptions.RedisError as e:
        # Redis down / timeout / connection refused
        return pd.DataFrame(), pd.DataFrame(), None, f"Redis connection error: {e}"

    try:
        incidents_df = pd.DataFrame(json.loads(incidents_raw)) if incidents_raw else pd.DataFrame()
        counts_df = pd.DataFrame(json.loads(counts_raw)) if counts_raw else pd.DataFrame()
    except (json.JSONDecodeError, TypeError) as e:
        return pd.DataFrame(), pd.DataFrame(), None, f"Redis payload error: {e}"

    return incidents_df, counts_df, last_update, None


st.title("Live Weather and Traffic Incidents in St. Louis, MO")
st.write("Displays traffic incidents based on weather in near real time.")
st_autorefresh(interval=2000, key="redis_auto_refresh")
st.caption("Auto-refreshing every 2 seconds for live updates.")

incidents_df, counts_df, last_update, redis_error = load_from_redis()

if redis_error:
    st.error(redis_error)
    st.info("Make sure Redis is running: docker compose up -d")
    st.stop()

if incidents_df.empty:
    st.warning("No Redis data yet. Start api_ingestion.py and spark_streaming.py, then refresh.")
    st.stop()

st.success(f"Live cache loaded from Redis. Last update: {last_update}")

st.subheader("Recent Traffic Incidents")
st.dataframe(incidents_df, use_container_width=True)

st.subheader("Incidents by Weather Condition")
if not counts_df.empty:
    x_col = "weather_desc" if "weather_desc" in counts_df.columns else "WEATHER_DESC"
    y_col = "count" if "count" in counts_df.columns else "INCIDENT_COUNT"
    st.bar_chart(data=counts_df, x=x_col, y=y_col)
else:
    st.write("No incident count data yet.")