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
charts = ['market-price', 'difficulty', 'hash-rate', 'miners-revenue', 'transaction-fees-usd',
          'transaction-fees', 'n-transactions', 'n-transactions-per-block']
task_list = list()



with (DAG(
    schedule='@weekly',
    dag_id="btc_network_data",
    catchup=True,
    start_date=datetime(2025, 10, 2),
    end_date=datetime(2025, 10, 13),
    max_active_runs=1,
    tags=["btc_network_data"]
)):
    @task()
    def extract(url, timespan, rolling_average, **kwargs):
        start_unix_timestamp = int(
            (kwargs["data_interval_start"] - timedelta(days=2)).timestamp()
        )

        print('start_unix_timestamp: ', start_unix_timestamp)

        charts_value = {}

        for chart in charts:

            print('LINK: ', f"{url}{chart}?start={start_unix_timestamp}&timespan={timespan}days&rollingAverage={rolling_average}days&format=json")

            response = requests.get(
                f"{url}{chart}?start={start_unix_timestamp}&timespan={timespan}days&rollingAverage={rolling_average}days&format=json")

            if response.status_code == 200:
                date = datetime.fromtimestamp(response.json()["values"][0]["x"]).strftime("%m/%d/%Y")
                value = round(response.json()["values"][0]["y"])

                credentials_path = '/opt/airflow/creds/btc-network-data-production-6d9e3665add0.json'

                spreadsheet_id = '1BMy2N_WZ5ivsY7K_EXbMMJt8lZeUAgtmeIxWcVmZjH8'

                scope = [
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]

                charts_value[f"{chart}"] = value

            else:
                raise AirflowException(f"Failed to get {chart}. Status code: {response.status_code}")

        print(charts_value)

        final_data = []

        for chart_key, chart_value in charts_value.items():
            if chart_key == 'n-transactions':
                    final_data.append(round(int(charts_value['transaction-fees-usd']) / int(charts_value['n-transactions']), 2))
            elif chart_key == 'difficulty':
                    final_data.append(round(int(chart_value) / 1000000000000, 1))
            else:
                final_data.append(chart_value)


        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(spreadsheet_id).worksheet("bitcoin_stats")

        final_data.insert(0, date)
        final_data.insert(4, round(charts_value["miners-revenue"] / charts_value["market-price"]))
        final_data.insert(9, round(charts_value["transaction-fees-usd"] /
                          (charts_value["miners-revenue"] - charts_value["transaction-fees-usd"]) * 100, 2))
        final_data.insert(10, round(charts_value["miners-revenue"] / charts_value["hash-rate"] * 1000))
        final_data.insert(11, charts_value["transaction-fees-usd"] /
                          (int(charts_value['transaction-fees-usd']) / int(charts_value['n-transactions'])))
        final_data.insert(13, round(final_data[11] / charts_value["n-transactions-per-block"]))
        final_data.insert(14, round(final_data[4] / final_data[13], 1))
        final_data.insert(15, round(final_data[13]  * 10 / 144, 2))
        sheet.append_row(final_data)

    extract(URL,'1', '1')
