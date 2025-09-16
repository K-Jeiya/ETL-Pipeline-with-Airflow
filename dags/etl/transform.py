import pandas as pd 
from airflow.providers.mysql.hooks.mysql import MySqlHook

def transform_data(**kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='mysql_complaints')

    # for selecting all data 
    sql = "SELECT * FROM consumer_complaints"
    # fetch in pandas dataframes
    df = mysql_hook.get_pandas_df(sql)
    print("data fetch from mysql:", df.shape)

    # drop columns
    drop_col = [
        "complaint_what_happened",
        "date_sent_to_company",
        "zip_code",
        "tags",
        "has_narrative",
        "consumer_consent_provided",
        "consumer_disputed",
        "company_public_response"
    ]
    df = df.drop(columns=drop_col, errors="ignore")
    print("after droping columns:", df.shape)

    # month-year column 
    df["month_year"] = pd.to_datetime(df["date_received"], errors="coerce").dt.strftime("%m-%y")
    print("created month_year col")

    # grouping
    group_cols = [
        "product",
        "issue",
        "sub_product",
        "timely",
        "company_response",
        "submitted_via",
        "company",
        "state",
        "sub_issue",
        "month_year"
    ]
    # null value to not availble to not skip 
    df[group_cols] = df[group_cols].fillna("Not Available")

    # distinct count
    transform_df = df.groupby(group_cols, dropna=False).agg(
        complaint_count=("complaint_id", "nunique")
    ).reset_index()
    print("transformation is complete", transform_df.shape)

    # save transforn data in csv 
    transform_df.to_csv('/home/jeiyakumari/airflow/dags/etl/data/Consumer_Complaints_Transformed.csv', index=False)
    print("saved successfully")

    return "CSV saved."