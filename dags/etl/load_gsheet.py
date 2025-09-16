import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import os

def load_gsheet(**kwargs):
    # CSV file path
    csv_file = "/home/jeiyakumari/airflow/dags/etl/data/Consumer_Complaints_Transformed.csv"

    # Check if file exists
    if not os.path.exists(csv_file):
        print("CSV file not found!")
        return

    # Read CSV 
    df = pd.read_csv(csv_file)
    print(f"CSV loaded successfully, {len(df)} rows")
    # Gogle Service Account credentials
    creds_path = os.path.join(os.path.dirname(__file__), "gcp_creds.json")
    creds = Credentials.from_service_account_file(
        creds_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )

    # build service
    service = build("sheets", "v4", credentials=creds)
    SHEET_ID = "1nzV4dvDkGzxTWIfWMwN1ehy6NB7OByWgDMBVGKSblk8"
    RANGE_NAME = "Sheet1!A1"
    data = [df.columns.tolist()] + df.values.tolist()

    # Clear sheet data
    service.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID,
        range=RANGE_NAME
    ).execute()
    # upload new data
    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=RANGE_NAME,
        valueInputOption="RAW",
        body={"values": data}
    ).execute()

    print("data uploaded to googlesheet")
