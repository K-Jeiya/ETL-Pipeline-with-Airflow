import os
import pandas as pd
from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def load_to_mysql(**kwargs):
    # xcom se path get 
    ti = kwargs['ti']
    csv_path = ti.xcom_pull(task_ids='extract_data_from_source')

    if not csv_path:
        print("no file path")
        return

    print("load data from:", csv_path)

    # read csv file 
    df = pd.read_csv(csv_path)
    print("sucessuffly read", len(df))

    # if nan then replace to none 
    df = df.where(pd.notnull(df), None)

    # sql
    mysql_hook = MySqlHook(mysql_conn_id='mysql_complaints')

    # Insert rows
    # Insert rows
    for _, row in df.iterrows():
        mysql_hook.run(
            sql="""
            INSERT INTO consumer_complaints 
            (product, issue, sub_product, complaint_id, timely, company_response, submitted_via, company, date_received, state, sub_issue)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            parameters=[
                row['product'], 
                row['issue'], 
                row['sub_product'], 
                row['complaint_id'], 
                row['timely'], 
                row['company_response'], 
                row['submitted_via'], 
                row['company'], 
                row['date_received'], 
                row['state'], 
                row['sub_issue']
            ],
            autocommit=True
        )