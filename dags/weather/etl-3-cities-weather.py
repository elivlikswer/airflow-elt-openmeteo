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
cities_config = None



#func
def extract_config_cities(city_name):
    """
    Checks for Empty config variable, is it's - load from Airflow only once. Then just return lat and lon by the specified city
    """
    global cities_config
    if cities_config is None:
        cities_config = Variable.get('data', deserialize_json=True)

    return  cities_config[city_name]['lat'],cities_config[city_name]['lon']


def get_temp_from_api(lat: str, lon:str):
    temp_api_url = Variable.get('api_url')
    print('temp_api_url: ',temp_api_url)
    api_url = temp_api_url.format(lat=lat,lon=lon)
    print('Api URL is ',api_url)
    hook = HttpHook(http_conn_id='open_meteo',method='GET')
    response = hook.run(api_url)
    return response.json()


#extract
def extract_sydney():
    city = 'Sydney'
    lat, lon = extract_config_cities(city)
    print(lat,lon)
    data = get_temp_from_api(lat,lon)
    temp = data['current_weather']['temperature']
    return city,temp

def extract_london():
    city = 'London'
    lat, lon = extract_config_cities(city)
    print(lat,lon)
    data = get_temp_from_api(lat, lon)
    temp = data['current_weather']['temperature']
    return city,temp

def extract_tokyo():
    city = 'Tokyo'
    lat, lon = extract_config_cities(city)
    print(lat,lon)
    data = get_temp_from_api(lat, lon)
    temp = data['current_weather']['temperature']
    return city,temp

#transform:
#{city_name: [temp,f_temp,status]}...

def transform_all(ti):
    total_data = {}
    for city, temp in ti.xcom_pull(task_ids=['extract_sydney', 'extract_tokyo', 'extract_london']):
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
    ds = logical_date.strftime('%Y=%m-%d')
    df.to_csv(f'weather_csv_{ds}',index=False)
    return df

#report
def log_report_weather(ti):
    print('='*40)
    print('     DAILY REPORT FOR WEATHER      ')
    print('='*40)

    df = ti.xcom_pull(task_ids='load_all')
    print(df.to_string(index=False))

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
    task_extract_sydney = PythonOperator(
        task_id='extract_sydney',
        python_callable=extract_sydney
    )

    task_extract_tokyo = PythonOperator(
        task_id='extract_tokyo',
        python_callable=extract_tokyo
    )

    task_extract_london = PythonOperator(
        task_id='extract_london',
        python_callable=extract_london
    )

    task_transform_all = PythonOperator(
        task_id='transform_all',
        python_callable=transform_all
    )

    task_load_all = PythonOperator(
        task_id='load_all',
        python_callable=pandas_load_csv
    )

    task_report_log = PythonOperator(
        task_id='report_log',
        python_callable=log_report_weather
    )

    start>>[task_extract_sydney,task_extract_tokyo,task_extract_london]>>task_transform_all>>task_load_all>>task_report_log>>end