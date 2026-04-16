import json
import os
import pandas as pd
import redis
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv
load_dotenv()

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


def load_history_from_snowflake(limit=500):
    try:
        with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), "rb") as key_file:
            passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
            password_bytes = passphrase.encode() if passphrase else None

            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=password_bytes,
                backend=default_backend()
            )
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            private_key=pkb,
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE")
        )

        query = f"""
            SELECT T_ID, T_START, WEATHER_DESC
            FROM WEATHER_TRAFFIC_COMB
            ORDER BY T_START DESC
            LIMIT {limit}
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df, None
    except Exception as e:
        return pd.DataFrame(), str(e)

#------------------------------------------------------#
#             Redis Live Stream Section                #
#------------------------------------------------------#

st.title("Live Weather and Traffic Incidents in St. Louis, MO")
st.write("Displays traffic incidents based on weather in near real time.")
st_autorefresh(interval=2000, key="redis_auto_refresh")
st.caption("Auto-refreshing every 2 seconds for live updates.")

st.subheader("Live Redis Cache (Recent Window)")
incidents_df, counts_df, last_update, redis_error = load_from_redis()

# if redis_error:
#     st.error(redis_error)
#     st.info("Make sure Redis is running: docker compose up -d")
#     st.stop()

# if incidents_df.empty:
#     st.warning("No Redis data yet. Start api_ingestion.py and spark_streaming.py, then refresh.")
#     st.stop()

# st.success(f"Live cache loaded from Redis. Last update: {last_update}")

# st.subheader("Recent Traffic Incidents")
# st.dataframe(incidents_df, use_container_width=True)

# st.subheader("Incidents by Weather Condition")
# if not counts_df.empty:
#     x_col = "weather_desc" if "weather_desc" in counts_df.columns else "WEATHER_DESC"
#     y_col = "count" if "count" in counts_df.columns else "INCIDENT_COUNT"
#     st.bar_chart(data=counts_df, x=x_col, y=y_col)
# else:
#     st.write("No incident count data yet.")

if redis_error:
    st.error(redis_error)
    st.info("Make sure Redis is running: docker compose up -d")
else:
    if incidents_df.empty:
        st.warning("No Redis live data yet. Start api_ingestion.py and spark_streaming.py.")
    else:
        st.success(f"Live cache loaded from Redis. Last update: {last_update}")

        st.markdown("Recent Traffic Incidents")
        st.dataframe(incidents_df, use_container_width=True)

        st.markdown("Incidents by Weather Condition (Live)")
        if not counts_df.empty:
            x_col = "weather_desc" if "weather_desc" in counts_df.columns else "WEATHER_DESC"
            y_col = "count" if "count" in counts_df.columns else "INCIDENT_COUNT"
            st.bar_chart(data=counts_df, x=x_col, y=y_col)
        else:
            st.write("No live incident count data yet.")

st.divider()

#------------------------------------------------------#
#            Snowflake Historical Section              #
#------------------------------------------------------#

st.subheader("Historical Data from Snowflake")

history_df, sf_error = load_history_from_snowflake(limit=1000)

if sf_error:
    st.error(f"Snowflake error: {sf_error}")
else:
    st.success(f"Loaded {len(history_df)} historical rows from Snowflake.")
    st.dataframe(history_df, use_container_width=True)

    if not history_df.empty and "WEATHER_DESC" in history_df.columns:
        hist_counts = (
            history_df.groupby("WEATHER_DESC")
            .size()
            .reset_index(name="INCIDENT_COUNT")
        )
        st.markdown("Incidents by Weather Condition (Historical)")
        st.bar_chart(hist_counts, x="WEATHER_DESC", y="INCIDENT_COUNT")