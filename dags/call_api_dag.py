from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from datetime import datetime
import logging
import json
import requests
import os
from dotenv import load_dotenv

load_dotenv(".env")
logger = logging.getLogger(__name__)

def _get_air_polution_report_today():
    url = f"https://api.waqi.info/feed/bangkok/?token={Variable.get('TOKEN')}"
    response = requests.request("GET", url)
    data = response.json()
    return data

def _transform_the_data_polution(ti):

    data = ti.xcom_pull(task_ids="get_air_polution_report_today")
    
    iaqi = data["data"]["iaqi"]
    time = data["data"]["time"]
    co = iaqi["co"]["v"]
    h = iaqi["h"]["v"]
    pm10 = iaqi["pm10"]["v"]
    pm25 = iaqi["pm25"]["v"]
    time_s = time["s"]
    time_iso = time["iso"]

    result = {
        "timestamp_utc" : time_s,
        "timestamp_tz" : time_iso,
        "co" : co,
        "h" : h,
        "pm10": pm10,
        "pm25" : pm25
    }
    
    with open("data_pm.json", "w") as f:
        json.dump(result, f)

    return result

default_args = {
    'owner': 'my-secound-dags',
    'start_date': days_ago(1),
}

with DAG('polution_data_pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         description='The pipeline for AIR POLITIONcd report', 
         catchup=False) as dags:
    
    t1 = PythonOperator(
        task_id='get_air_polution_report_today',
        python_callable=_get_air_polution_report_today
    )

    t2 = PythonOperator(
        task_id='transform_the_data_polution',
        python_callable=_transform_the_data_polution
    )

    t1 >> t2
