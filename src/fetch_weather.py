import json
import os
from datetime import date

import requests

CITIES = {
    "Hyderabad": (17.385, 78.4867),
    "Mumbai": (19.076, 72.8777),
    "Delhi": (28.6139, 77.209),
    "Bengaluru": (12.9716, 77.5946),
    "Chennai": (13.0827, 80.2707),
    "Kolkata": (22.5726, 88.3639),
}

URL = "https://api.open-meteo.com/v1/forecast"


def fetch(city, lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "past_days": 1,
        "forecast_days": 1,
        "timezone": "Asia/Kolkata",
    }
    response = requests.get(URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    data["city"] = city
    return data


if __name__ == "__main__":
    os.makedirs("output", exist_ok=True)
    for city, (lat, lon) in CITIES.items():
        data = fetch(city, lat, lon)
        path = f"output/{city}_{date.today()}.json"
        with open(path, "w") as f:
            json.dump(data, f)
        print(f"Saved {path}: {len(data['hourly']['time'])} hourly rows")
        