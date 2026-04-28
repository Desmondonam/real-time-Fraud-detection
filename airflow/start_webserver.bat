@echo off
set AIRFLOW_HOME=C:\Users\Admin\Desktop\projects\Real-time Fraud Detection\airflow
set AIRFLOW__CORE__EXECUTOR=SequentialExecutor
set AIRFLOW__CORE__LOAD_EXAMPLES=False
set AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:desmond316@localhost:5432/airflow_metadata
set AIRFLOW__CORE__DAGS_FOLDER=C:\Users\Admin\Desktop\projects\Real-time Fraud Detection\airflow\dags
set AIRFLOW__LOGGING__BASE_LOG_FOLDER=C:\Users\Admin\Desktop\projects\Real-time Fraud Detection\airflow\logs
C:\Users\Admin\.conda\envs\myenv\Scripts\airflow.exe webserver --port 8080
