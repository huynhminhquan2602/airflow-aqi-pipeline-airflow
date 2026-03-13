# Airflow AQI Data Pipeline

## Overview

This project builds a **data pipeline using Apache Airflow** to ingest **Air Quality Index (AQI)** data from the **IQAir AirVisual API**, transform it, and store it in a PostgreSQL database.

The pipeline is orchestrated using **Apache Airflow running on Docker**.

---

## Architecture

API → Airflow DAG → ETL (Python) → PostgreSQL

---

## Tech Stack

* Apache Airflow
* Docker & Docker Compose
* Python
* PostgreSQL
* AirVisual API
* DBeaver (for database inspection)

---

## Project Structure

```
airflow-aqi-pipeline
│
├── dags
│   ├── aqi_pipeline.py
│   └── aqi_etl.py
│
├── docker-compose.yml
├── requirements.txt
├── README.md
└── .env.example
```

---

## DAG Workflow

The DAG performs the following steps:

1. Fetch AQI data from AirVisual API
2. Transform pollution data
3. Insert city and pollution records into PostgreSQL
4. Log results for monitoring

---

## Example DAG Graph

(Add screenshot here)

---

## Setup Instructions

### 1 Clone repository

```
git clone https://github.com/yourusername/airflow-aqi-pipeline.git
cd airflow-aqi-pipeline
```

### 2 Setup environment variables

```
cp .env.example .env
```

Add your API key.

### 3 Start Airflow

```
docker compose up -d
```

### 4 Open Airflow UI

```
http://localhost:8080
```

Login:

```
username: airflow
password: airflow
```

### 5 Trigger DAG

Run DAG:

```
aqi_data_pipeline
```

---

## Database Schema

### city table

| column      | type    |
| ----------- | ------- |
| city_id     | integer |
| cityname    | text    |
| statename   | text    |
| countryname | text    |

### pollution table

| column   | type      |
| -------- | --------- |
| id       | integer   |
| cityid   | integer   |
| aqius    | integer   |
| mainus   | text      |
| datetime | timestamp |

---

## Testing

You can verify data using SQL:

```
SELECT * FROM city;
SELECT * FROM pollution;
```

---

## Future Improvements

* Add **Bronze / Silver / Gold data layers**
* Add **data quality checks**
* Add **dashboard visualization**
* Support **dynamic city discovery from API**

---

## Author

Quan Huynh
Aspiring Data Engineer
