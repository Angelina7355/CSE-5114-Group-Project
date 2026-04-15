# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title(f"Live Weather and Traffic Incidents in Saint Louis")
st.write(
  """
  Displays traffic incidents based on the weather live.
  """
)

# Get the current credentials
session = get_active_session()
df = session.sql("SELECT * FROM WEATHER_TRAFFIC_COMB ORDER BY T_START DESC").to_pandas()
st.subheader("Traffic List Based on Weather")
st.dataframe(df)

weather_counts = df.groupby("WEATHER_DESC").size().reset_index(name="INCIDENT_COUNT")
st.subheader("Incidents by Weather Condition")
st.bar_chart(data=weather_counts, x="WEATHER_DESC", y="INCIDENT_COUNT", x_label="Weather Description", y_label="Incident Count")

df_rate = session.sql("""
    SELECT 
        wd.WEATHER_DESC,
        COUNT(DISTINCT wtc.T_ID) / COUNT(DISTINCT wd.W_TIMESTAMP) as INCIDENT_RATE
    FROM WEATHER_DURATION wd
    LEFT JOIN WEATHER_TRAFFIC_COMB wtc ON wd.WEATHER_DESC = wtc.WEATHER_DESC
    GROUP BY wd.WEATHER_DESC
""").to_pandas()

st.subheader("Incident Rate by Weather Condition (per mins)")
st.bar_chart(data=df_rate, x="WEATHER_DESC", y="INCIDENT_RATE")
