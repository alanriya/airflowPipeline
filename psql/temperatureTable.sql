CREATE SCHEMA IF NOT EXISTS airflow;

CREATE TABLE IF NOT EXISTS airflow.sg_weather_temperature(
    ts timestamptz,
    location VARCHAR,
    longitude numeric,
    latitude numeric,
    station_id VARCHAR,
    reading numeric,
    units VARCHAR
);