from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
from airflow.exceptions import AirflowException
from airflow.sdk import Variable
from telegram_notification import TelegramNotification
import pandas as pd


URL = Variable.get("url_blockchain")
CHARTS = Variable.get("charts", deserialize_json=True)
SPREADSHEET_ID = Variable.get("spreadsheet_id")
SCOPE = Variable.get("scope", deserialize_json=True)
CREDENTIAL_PATH = Variable.get("path_to_creds")

default_args = {
    "on_failure_callback": TelegramNotification.send_message_error
}

@dag(
    schedule='0 8 * * 1',
    catchup=True,
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    max_active_runs=1,
    tags=["btc_network_data"]
)
def btc_network_data():
    @task
    def check_date():
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIAL_PATH, SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet("raw_data")
        values_list = sheet.col_values(1)
        return values_list

    @task()
    def do_extract(url: str, timespan: str, rolling_average: str, exists_dates: list, **kwargs):
        start_unix_timestamp = int((kwargs["data_interval_start"] - timedelta(days=float(timespan))).timestamp())
        date_end = (datetime.fromtimestamp(start_unix_timestamp) + timedelta(days=float(timespan))).strftime("%m/%d/%Y")

        print("start_unix_timestamp: ", start_unix_timestamp)
        print("start_unix_timestamp_DATE: ", datetime.fromtimestamp(start_unix_timestamp).strftime("%m/%d/%Y"))

        for chart in CHARTS:
            print(f"{url}{chart}?start={start_unix_timestamp}&"
                  f"timespan={timespan}days&rollingAverage={rolling_average}days&format=json")

        if date_end in exists_dates:
            raise AirflowSkipException("Такие даты уже есть в датасете")

        charts_value = {}

        responses = [requests.get(f"{url}{chart}?start={start_unix_timestamp}&"
                f"timespan={timespan}days&rollingAverage={rolling_average}days&format=json") for chart in CHARTS]

        for response in responses:
            if response.status_code == 200:
                charts_value[f"{response.json()['name']}"] = response.json()["values"]
            else:
                raise AirflowException(f"Failed to get {response.json()['name']}. Status code: {response.status_code}")

        print(charts_value)

        return charts_value

    @task()
    def prepare_data_for_gspread(extracted_dict: dict):
        df = pd.DataFrame(data=extracted_dict)

        df["date_for_rows"] = df["Market Price (USD)"].apply(lambda x: x["x"])
        df = df.map(
            lambda x: x["y"] if isinstance(x, dict) else datetime.fromtimestamp(x).strftime("%m/%d/%Y")
        )
        date_col = ["date_for_rows"]
        other_cols = sorted(c for c in df.columns if c != "date_for_rows")
        df = df[date_col + other_cols]
        final_data = df.values.tolist()

        return final_data

    @task()
    def write_to_sheet(data: list):
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIAL_PATH, SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet("raw_data")
        sheet.append_rows(data)

    extract_data = do_extract(URL,'7', '1', check_date())
    data_for_gspread = prepare_data_for_gspread(extract_data)
    write_to_sheet(data_for_gspread)

btc_network_data()
