-- Runs once, on first start of an empty Postgres volume.
-- The application database comes from POSTGRES_DB; Airflow needs its own.
CREATE DATABASE airflow_metadata;
