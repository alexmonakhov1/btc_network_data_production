from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from airflow.providers.telegram.operators.telegram import TelegramOperator
import requests
from airflow.exceptions import AirflowException
from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.models.dagrun import DagRun
import os



URL = 'https://api.blockchain.info/charts/'
charts = ['hash-rate', 'miners-revenue', 'transaction-fees-usd', 'transaction-fees',
          'n-transactions-per-block','difficulty']
task_list = list()



with DAG(
    schedule='@daily',
    dag_id="btc_network_data",
    catchup=True,
    start_date=datetime(2025, 12, 10),
    tags=["btc_network_data"]
):
    @task()
    def extract(url, chart, timespan, rolling_average, **kwargs):
        data_interval_start = kwargs["data_interval_start"]
        start_unix_timestamp = int(
            (data_interval_start - timedelta(days=2)).timestamp()
        )

        print('start_unix_timestamp: ', start_unix_timestamp)
        print('data_interval_start: ', data_interval_start)

        print('LINK: ', f"{url}{chart}?start={start_unix_timestamp}&timespan={timespan}days&rollingAverage={rolling_average}days&format=json")

        response = requests.get(
            f"{url}{chart}?start={start_unix_timestamp}&timespan={timespan}days&rollingAverage={rolling_average}days&format=json")

        if response.status_code == 200:
            date = datetime.fromtimestamp(response.json()["values"][0]["x"]).date().isoformat()
            value = round(response.json()["values"][0]["y"])

            credentials_path = '/opt/airflow/creds/btc-network-data-production-6d9e3665add0.json'

            spreadsheet_id = '1BMy2N_WZ5ivsY7K_EXbMMJt8lZeUAgtmeIxWcVmZjH8'

            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]

            creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
            client = gspread.authorize(creds)

            # Открываем таблицу
            sheet = client.open_by_key(spreadsheet_id).sheet1

            data = [date, chart, value]

            sheet.append_row(data)

        else:
            raise AirflowException(f"Failed to get {chart}. Status code: {response.status_code}")

    extract(URL, 'hash-rate','1', '1')
