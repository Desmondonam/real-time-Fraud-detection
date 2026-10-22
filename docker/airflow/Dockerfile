FROM apache/airflow:2.10.4-python3.12

# Same scikit-learn / numpy pins as requirements.txt so the model bundle is
# portable between the Airflow and app containers. pandas, SQLAlchemy and
# psycopg2 already ship with the Airflow image.
RUN pip install --no-cache-dir \
    "numpy==1.26.4" "scipy==1.14.1" "scikit-learn==1.5.2" "joblib==1.4.2" "python-dotenv==1.0.1"
