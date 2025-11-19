# API Meteostat
from meteostat import Daily, Stations
from datetime import datetime
import pandas as pd

def extract_weather(start="2024-01-01", end="2024-12-31"):
    nyc = Stations().nearby(40.7128, -74.0060).fetch(1)
    data = Daily(nyc.iloc[0], start=start, end=end).fetch()
    data.reset_index(inplace=True)
    data.rename(columns={
        "time": "date_key",
        "tavg": "tavg_c",
        "prcp": "prcp_mm",
        "wspd": "wind_kph"
    }, inplace=True)
    data.to_csv("data/raw/weather/weather_nyc.csv", index=False)
    print("✅ Weather data saved to data/raw/weather/weather_nyc.csv")

if __name__ == "__main__":
    extract_weather()
