import sys
import os
sys.path.append(os.path.dirname(__file__))

from airflow import DAG  
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.sensors.filesystem import FileSensor
from airflow.operators.email import EmailOperator

# import functions
from etl.extract import run as extract_run
from etl.load import load_to_mysql
from etl.transform import transform_data
from etl.load_gsheet import load_gsheet

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["jeiyakumari@gmail.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="etl_dag",
    default_args=default_args,
    description="ETL Assignment",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),  
    catchup=False,
    tags=["etl", "consumer_complaints", "assignment"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data_from_source",
        python_callable=extract_run
    )
    load_task = PythonOperator(
        task_id="dump_data_to_mysql",
        python_callable=load_to_mysql
    )
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )
    check_task = FileSensor(
        task_id="check_consumer_financial_csv",
        filepath="Consumer_Complaints_Transformed.csv",  
        poke_interval=10,
        timeout=300,
        mode='poke',        # or 'reschedule' to free the worker
        fs_conn_id='fs_default',  # this is the connection you just created
    )
    load_gsheet_task = PythonOperator(
        task_id="dump_googlesheet",
        python_callable=load_gsheet
    )
    email_send_task = EmailOperator(
        task_id="send_googlesheet_url_via_email",
        to="jeiyakumari@gmail.com",
        subject="Sharing consumer complaints googlesheet url",
        html_content=f"""
        <h3>Hello! <br> You are doing well.</h3>
        <p>The Consumer Complaints data has been uploaded to Google Sheets.</p>
        <p>Access it here: <a href="https://docs.google.com/spreadsheets/d/1nzV4dvDkGzxTWIfWMwN1ehy6NB7OByWgDMBVGKSblk8/edit?gid=0#gid=0">Google Sheet Link</a></p>
        """
    )
    
    extract_task >> load_task >> transform_task >> check_task >> load_gsheet_task >> email_send_task