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

@dag(
    schedule='@weekly',
    catchup=True,
    start_date=datetime(2025, 10, 2),
    end_date=datetime(2025, 11, 3),
    max_active_runs=1,
    tags=["btc_network_data"]
)
def btc_network_data():
    @task()
    def do_extract(url, timespan, rolling_average, **kwargs):
        charts_value = {}
        start_unix_timestamp = int(
            (kwargs["data_interval_start"] - timedelta(days=2)).timestamp()
        )

        for chart in charts:
            print('LINK: ', f"{url}{chart}?start={start_unix_timestamp}&timespan={timespan}days&rollingAverage={rolling_average}days&format=json")

            response = requests.get(
                f"{url}{chart}?start={start_unix_timestamp}&timespan={timespan}days&rollingAverage={rolling_average}days&format=json")

            if response.status_code == 200:
                charts_value[f"{chart}"] = response.json()["values"]
            else:
                raise AirflowException(f"Failed to get {chart}. Status code: {response.status_code}")

        return charts_value

    @task()
    def do_data_for_gspread(dict):
        final_data = []

        charts = list(dict.keys())
        length = len(dict[charts[0]])

        for i in range(length):
            row = []
            timestamp = datetime.fromtimestamp(dict[charts[0]][i]['x']).strftime("%m/%d/%Y")
            row.append(timestamp)

            for chart in charts:
                if chart == 'n-transactions':
                    row.append(dict['transaction-fees-usd'][i]['y'] / dict['transaction-fees-usd'][i]['y'])
                elif chart == 'difficulty':
                    row.append(dict[chart][i]['y'] / 1000000000000)
                else:
                    row.append(dict[chart][i]['y'])

            final_data.append(row)

        # final_data.insert(0, date)
        # final_data.insert(4, round(charts_value["miners-revenue"] / charts_value["market-price"]))
        # final_data.insert(9, round(charts_value["transaction-fees-usd"] /
        #                            (charts_value["miners-revenue"] - charts_value["transaction-fees-usd"]) * 100, 2))
        # final_data.insert(10, round(charts_value["miners-revenue"] / charts_value["hash-rate"] * 1000))
        # final_data.insert(11, charts_value["transaction-fees-usd"] /
        #                   (int(charts_value['transaction-fees-usd']) / int(charts_value['n-transactions'])))
        # final_data.insert(13, round(final_data[11] / charts_value["n-transactions-per-block"]))
        # final_data.insert(14, round(final_data[4] / final_data[13], 1))
        # final_data.insert(15, round(final_data[13] * 10 / 144, 2))

        return final_data

    @task()
    def write_to_sheet(data):
        credentials_path = '/opt/airflow/creds/btc-network-data-production-6d9e3665add0.json'
        spreadsheet_id = '1BMy2N_WZ5ivsY7K_EXbMMJt8lZeUAgtmeIxWcVmZjH8'
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(spreadsheet_id).worksheet("bitcoin_stats")

        sheet.append_rows(data)

    extract_data = do_extract(URL,'7', '1')
    data_for_gspread = do_data_for_gspread(extract_data)
    write_to_sheet(data_for_gspread)

btc_network_data()
