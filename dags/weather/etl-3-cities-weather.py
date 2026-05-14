from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Variable

import pandas as pd

from datetime import datetime
from pathlib import Path

#base_var
DAG_ID = Path(__file__).stem.lower()
CSV_STORAGE_DIR = '/opt/airflow/logs/weather_reports'


#func
def extract_config_cities(city):
    city_config = Variable.get('data', deserialize_json=True)
    return  city_config[city]['lat'],city_config[city]['lon']


def get_temp_from_api(lat: str, lon:str):
    temp_api_url = Variable.get('api_url')
    api_url = temp_api_url.format(lat=lat,lon=lon)
    hook = HttpHook(http_conn_id='open_meteo',method='GET')
    response = hook.run(api_url)
    return response.json()


#extract
def extract_cities(city):
    lat,lon = extract_config_cities(city)
    temp = get_temp_from_api(lat,lon)['current_weather']['temperature']
    return city,temp

#transform:
#{city_name: [temp,f_temp,status]}...

def transform_all(extract_task_ids,ti):
    total_data = {}
    for city, temp in ti.xcom_pull(task_ids=extract_task_ids):
        fahrenheit = (temp*1.8)+32
        if temp < 10:
            status = 'cold'
        elif temp > 25:
            status  = 'hot'
        else:
            status = 'comfortable'

        total_data[city] = [temp,fahrenheit,status]
    return total_data

#load
def pandas_load_csv(ti,logical_date=None):
    flat_data = []
    raw_data = ti.xcom_pull(task_ids='transform_all')
    for city, metrics in raw_data.items():
        flat_data.append(
            {
                'city':city,
                'temp_c':metrics[0],
                'temp_f':metrics[1],
                'status':metrics[2]
            }
        )

    df = pd.DataFrame(flat_data)
    ds = logical_date.strftime('%Y-%m-%d')
    filename=f'weather_csv_{ds}'
    filepath = Path(CSV_STORAGE_DIR) / filename
    Path(CSV_STORAGE_DIR).mkdir(parents=True,exist_ok=True)
    df.to_csv(filepath,index=False)
    return str(filepath)

#report
def log_report_weather(ti):
    print('='*40)
    print('     DAILY REPORT FOR WEATHER      ')
    print('='*40)

    csv_path = ti.xcom_pull(task_ids='load_all')

    df = pd.read_csv(csv_path,encoding='utf-8')

    print(df.to_string())

    print('='*40)
    print(f'Total cities processed: {len(df)}')




with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026,5,12),
    schedule='0 12 * * *',
    catchup=False
) as dag:

    #empty
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    #python
    CITIES = list(Variable.get('data',deserialize_json=True).keys())

    extract_tasks = []
    extract_tasks_ids = []
    for city in CITIES:
        task = PythonOperator(
            task_id=f'extract_{city.lower()}',
            python_callable=extract_cities,
            op_args=[city]
        )
        extract_tasks.append(task)
        extract_tasks_ids.append(task.task_id)


    task_transform_all = PythonOperator(
        task_id='transform_all',
        python_callable=transform_all,
        op_kwargs={'extract_task_ids': extract_tasks_ids}
    )

    task_load_all = PythonOperator(
        task_id='load_all',
        python_callable=pandas_load_csv
    )

    task_report_log = PythonOperator(
        task_id='report_log',
        python_callable=log_report_weather
    )

    start>>extract_tasks>>task_transform_all>>task_load_all>>task_report_log>>end