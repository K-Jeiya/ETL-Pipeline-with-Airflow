import os
import requests
import pandas as pd
from datetime import date, timedelta
from dotenv import load_dotenv

# env file load karna
load_dotenv()

def run(**kwargs):
    api_url = os.getenv("API_URL")
    size = int(os.getenv("API_SIZE", 500))
    states_url = os.getenv("STATES_URL")

    #  User-Agent add kiya taake error solve ho jaye
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115.0 Safari/537.36"
        )
    }
    # print(f" Fetching states ...")

    # for states
    states_resp = requests.get(states_url, headers=headers)
    states_resp.raise_for_status()
    states = states_resp.json() 
    # print(f"Total states fetched: {len(states)}")

    # Date for 1 month
    today = date.today()
    last_month = today - timedelta(days=30)

    all_data = []
    # for every state
    for sc in states.keys():
        params = {
            "size": size,
            "searchField": "state",
            "searchText": sc,
            "date_received_min": last_month.isoformat(),
            "date_received_max": today.isoformat()
        }

        r = requests.get(api_url, params=params, headers=headers)
        data = r.json().get("hits", {}).get("hits", [])
        all_data.extend(data)
        # print("done state records")

    # for store data in csv file
    if all_data:
        df = pd.DataFrame([d["_source"] for d in all_data])  
        file_path = os.path.expanduser("~/airflow/dags/etl/data/Consumer_Complaints.csv")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df.to_csv(file_path, index=False)
        print(f"Saved {len(df)} records")

        return file_path
    else:
        print("No data fetched!")
        return None

if __name__ == "__main__":
    run()
