from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


INPUT_FILE = "/opt/airflow/dags/doctors.csv"
OUTPUT_FILE = "/opt/airflow/dags/doctors_clean.csv"


def start_etl():
    print("Starting Doctor ETL Process...")


def read_csv():
    df = pd.read_csv(INPUT_FILE)

    print("\nOriginal Dataset:")
    print(df)

    print(f"\nTotal Records: {len(df)}")


def clean_data():

    df = pd.read_csv(INPUT_FILE)

    original_count = len(df)

    # Remove duplicates
    df = df.drop_duplicates()

    # Replace ######## with empty values
    df = df.replace("########", pd.NA)

    # Fill missing department
    if "Department" in df.columns:
        df["Department"] = df["Department"].fillna("Unknown")

    cleaned_count = len(df)

    # Save cleaned file
    df.to_csv(OUTPUT_FILE, index=False)

    print("\nCleaning Completed")
    print(f"Original Records : {original_count}")
    print(f"Cleaned Records  : {cleaned_count}")
    print(f"Duplicates Removed: {original_count - cleaned_count}")

    print("\nCleaned Dataset:")
    print(df)


def end_etl():
    print("ETL Process Completed Successfully")
    print(f"Cleaned file saved at: {OUTPUT_FILE}")


with DAG(
    dag_id="doctor_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["doctor", "etl"],
) as dag:

    start_task = PythonOperator(
        task_id="start_etl",
        python_callable=start_etl
    )

    read_task = PythonOperator(
        task_id="read_csv",
        python_callable=read_csv
    )

    clean_task = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data
    )

    end_task = PythonOperator(
        task_id="end_etl",
        python_callable=end_etl
    )

    start_task >> read_task >> clean_task >> end_task