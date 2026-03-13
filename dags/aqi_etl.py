import os
import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# load env
load_dotenv("/opt/airflow/.env")

API_KEY = os.getenv("IQAIR_API_KEY")

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")


CITIES = [
    {"city": "Hanoi", "state": "Ha Noi", "country": "Vietnam"},
    {"city": "Hue", "state": "Tinh Thua Thien-Hue", "country": "Vietnam"}
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

    url = "http://api.airvisual.com/v2/city"

    params = {
        "city": city,
        "state": state,
        "country": country,
        "key": API_KEY
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise Exception(f"API error {response.status_code}: {response.text}")

    return response.json()


def run_aqi_etl():

    print("Starting AQI ETL pipeline...")

    conn = get_connection()
    cursor = conn.cursor()

    for c in CITIES:

        city = c["city"]
        state = c["state"]
        country = c["country"]

        try:

            print(f"Fetching AQI data for {city}...")

            data = fetch_aqi(city, state, country)

            pollution = data["data"]["current"]["pollution"]
            location = data["data"]["location"]["coordinates"]

            ts = pollution["ts"]
            aqius = pollution["aqius"]
            mainus = pollution["mainus"]
            aqicn = pollution["aqicn"]
            maincn = pollution["maincn"]

            latitude = location[1]
            longitude = location[0]

            # insert city
            cursor.execute(
                """
                INSERT INTO city (cityname, statename, countryname, latitude, longitude)
                VALUES (%s,%s,%s,%s,%s)
                RETURNING city_id
                """,
                (city, state, country, latitude, longitude)
            )

            city_id = cursor.fetchone()[0]

            # insert pollution
            cursor.execute(
                """
                INSERT INTO pollution
                (cityid, datetime, ts, aqius, mainus, aqicn, maincn)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    city_id,
                    datetime.utcnow(),
                    ts,
                    aqius,
                    mainus,
                    aqicn,
                    maincn
                )
            )

            conn.commit()

            print(f"Inserted AQI data for {city}")

        except Exception as e:

            conn.rollback()
            print(f"Error processing {city}: {e}")
            raise e

    cursor.close()
    conn.close()

    print("AQI ETL pipeline finished.")