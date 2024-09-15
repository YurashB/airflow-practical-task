import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task
from airflow.models import Variable

import pandas as pd


def log_message(message, level="INFO"):
    color_codes = {
        "INFO": "\033[94m",
        "WARNING": "\033[93m",
        "ERROR": "\033[91m",
        "RESET": "\033[0m"
    }

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    color = color_codes.get(level, color_codes["RESET"])
    formatted_message = f"{color}[{current_time}] [{level}] {message}{color_codes['RESET']}"

    print(formatted_message)


@dag(
    dag_id='airflow_task',
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description='Airflow practical task',
    schedule="@daily",
    start_date=datetime(2024, 9, 13, 3),
    catchup=True,
)
def airflow_etl():
    file_path = Variable.get("file_path", default_var="raw/AB_NYC_2019.csv")

    @task()
    def extract():
        if os.path.isfile(file_path) and os.access(file_path, os.R_OK):
            return pd.read_csv(file_path)
        else:
            raise FileNotFoundError(f"File {file_path} does not exist or is not readable.")

    @task()
    def transform(df: pd.DataFrame):
        df = df[df.price > 0]
        df["last_review"] = pd.to_datetime(df["last_review"])
        earliest_last_review = df["last_review"].min()
        df["last_review"] = df["last_review"].fillna(earliest_last_review)
        df["reviews_per_month"] = df["last_review"].fillna(0)
        df = df[df["reviews_per_month"].notna()]
        tf_file_path = Variable.get("tf_file_path", default_var="transformed/Transformed_AB_NYC_2019.csv")
        df.to_csv(Path(tf_file_path), index=False)
        return tf_file_path

    @task()
    def load(file_path):
        pg_hook = PostgresHook("postgres_airflow")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        for _, row in df.iterrows():
            print(row)
            cursor.execute("INSERT INTO airbnb_listings VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)",
            row)

        connection.commit()
        cursor.close()
        connection.close()
    df = extract()
    tf_file_path = transform(df)
    load(tf_file_path)


airflow_etl()
