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
    def extract(url, timespan, rolling_average, **kwargs):
        start_unix_timestamp = int(
            (kwargs["data_interval_start"] - timedelta(days=2)).timestamp()
        )

        print('start_unix_timestamp: ', start_unix_timestamp)

        charts_value = []

        for chart in charts:

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

                charts_value.append(value)

            else:
                raise AirflowException(f"Failed to get {chart}. Status code: {response.status_code}")

        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        client = gspread.authorize(creds)

        print(charts_value)

        sheet = client.open_by_key(spreadsheet_id).sheet1

        charts_value.insert(0, date)
        sheet.append_row(charts_value)

    extract(URL,'1', '1')
