import os
import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# load env variables
load_dotenv("/opt/airflow/.env")

API_KEY = os.getenv("IQAIR_API_KEY")

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

BASE_URL = "http://api.airvisual.com/v2/city"

# cities config
CITIES = [
    {"city": "Hanoi", "state": "Ha Noi", "country": "Vietnam"},
    {"city": "Lien Chieu", "state": "Hue", "country": "Vietnam"}
]


def get_connection():
    print("Connecting to PostgreSQL...")

    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

    print("Connected to PostgreSQL")
    return conn


def fetch_aqi(city, state, country):
    params = {
        "city": city,
        "state": state,
        "country": country,
        "key": API_KEY
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)

        if response.status_code != 200:
            print(f"API error for {city}: {response.text}")
            return None

        return response.json()

    except Exception as e:
        print(f"Request failed for {city}: {e}")
        return None


def run_aqi_etl():

    print("Starting AQI ETL pipeline...")

    conn = get_connection()
    cursor = conn.cursor()

    for c in CITIES:

        city = c["city"]
        state = c["state"]
        country = c["country"]

        print(f"Fetching AQI data for {city}...")

        data = fetch_aqi(city, state, country)

        if not data:
            print(f"Skipping {city} due to API error")
            continue

        try:

            pollution = data["data"]["current"]["pollution"]
            location = data["data"]["location"]["coordinates"]

            ts = pollution["ts"]
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))

            aqius = pollution["aqius"]
            mainus = pollution["mainus"]
            aqicn = pollution["aqicn"]
            maincn = pollution["maincn"]

            latitude = location[1]
            longitude = location[0]

            city_name = data["data"]["city"]
            state_name = data["data"]["state"]
            country_name = data["data"]["country"]

            # insert city
            cursor.execute(
                """
                INSERT INTO city (cityname, statename, countryname, latitude, longitude)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                RETURNING city_id
                """,
                (city_name, state_name, country_name, latitude, longitude)
            )

            result = cursor.fetchone()

            if result:
                city_id = result[0]
            else:
                cursor.execute(
                    "SELECT city_id FROM city WHERE cityname=%s",
                    (city_name,)
                )
                city_id = cursor.fetchone()[0]

            # insert pollution
            cursor.execute(
                """
                INSERT INTO pollution
                (cityid, datetime, ts, aqius, mainus, aqicn, maincn)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                (
                    city_id,
                    dt,
                    int(dt.timestamp()),
                    aqius,
                    mainus,
                    aqicn,
                    maincn
                )
            )

            conn.commit()

            print(f"Inserted AQI data for {city_name}")

        except Exception as e:

            conn.rollback()
            print(f"Database error for {city}: {e}")

    cursor.close()
    conn.close()

    print("AQI ETL pipeline finished")