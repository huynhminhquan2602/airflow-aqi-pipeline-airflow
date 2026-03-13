CREATE TABLE IF NOT EXISTS city (
    city_id SERIAL PRIMARY KEY,
    cityName TEXT,
    stateName TEXT,
    countryName TEXT,
    latitude FLOAT,
    longitude FLOAT
);

CREATE TABLE IF NOT EXISTS pollution (
    id SERIAL PRIMARY KEY,
    cityId INT,
    datetime TIMESTAMP,
    ts BIGINT,
    aqius INT,
    mainus TEXT,
    aqicn INT,
    maincn TEXT
);