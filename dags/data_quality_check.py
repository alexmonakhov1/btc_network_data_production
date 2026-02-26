import logging

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

DAYS_CHECK = 30

@dag(
    schedule='@daily',
    catchup=True,
    default_args=default_args,
    start_date=datetime(2026, 2, 21),
    max_active_runs=1,
    tags=["data_quality_check"]
)
def data_quality_check():
    @task
    def check_data_in_spreadsheet() -> str:
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIAL_PATH, SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet("test")

        last_row = Variable.get("test_last_row")
        first_row = int(last_row) - DAYS_CHECK

        values_list = sheet.get(f"A{first_row}:I{last_row}")
        logging.info(values_list)

        for row in values_list:
            if "0" in row:
                logging.info("LIST HAS ZERO!")
                logging.info("Data will be updated")
                return values_list[0][0]

        raise AirflowSkipException("All Data is so Good 🤤")

    @task
    def get_data_from_api(date: str) -> dict:






        


    check_data_in_spreadsheet()


data_quality_check()
