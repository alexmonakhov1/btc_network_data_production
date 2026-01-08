from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
from airflow.exceptions import AirflowException


URL = 'https://api.blockchain.info/charts/'
CHARTS = ['market-price', 'difficulty', 'hash-rate', 'miners-revenue', 'transaction-fees-usd',
          'transaction-fees', 'n-transactions', 'n-transactions-per-block']
CREDENTIAL_PATH = '/opt/airflow/creds/btc-network-data-production-6d9e3665add0.json'
SPREADSHEET_ID = '1BMy2N_WZ5ivsY7K_EXbMMJt8lZeUAgtmeIxWcVmZjH8'
SCOPE = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
]

@dag(
    schedule='@weekly',
    catchup=True,
    start_date=datetime(2025, 10, 1),
    end_date=datetime(2025, 10, 15),
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
        start_unix_timestamp = int((kwargs["data_interval_start"] - timedelta(days=2)).timestamp())
        date_end = (datetime.fromtimestamp(start_unix_timestamp) + timedelta(days=float(timespan))).strftime("%m/%d/%Y")

        if date_end in exists_dates:
            raise AirflowSkipException("Такие даты уже есть в датасете")
        charts_value = {}
        for chart in CHARTS:
            print('LINK: ', f"{url}{chart}?start={start_unix_timestamp}&"
                            f"timespan={timespan}days&rollingAverage={rolling_average}days&format=json")
            response = requests.get(
                f"{url}{chart}?start={start_unix_timestamp}&"
                f"timespan={timespan}days&rollingAverage={rolling_average}days&format=json")
            if response.status_code == 200:
                charts_value[f"{chart}"] = response.json()["values"]
            else:
                raise AirflowException(f"Failed to get {chart}. Status code: {response.status_code}")
        return charts_value

    @task()
    def do_data_for_gspread(dict: dict):
        final_data = []

        charts = list(dict.keys())
        length = len(dict[charts[0]])

        for i in range(length):
            row = []
            timestamp = datetime.fromtimestamp(dict[charts[0]][i]['x']).strftime("%m/%d/%Y")
            row.append(timestamp)

            for chart in charts:
                row.append(dict[chart][i]['y'])

            final_data.append(row)
        return final_data

    @task()
    def write_to_sheet(data: list):
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIAL_PATH, SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet("raw_data")
        sheet.append_rows(data)

    extract_data = do_extract(URL,'7', '1', check_date())
    data_for_gspread = do_data_for_gspread(extract_data)
    write_to_sheet(data_for_gspread)

btc_network_data()
