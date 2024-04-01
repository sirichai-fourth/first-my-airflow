# load deps
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import os
import logging
import pymysql
import requests
import json
from dotenv import load_dotenv

# load env
load_dotenv(".env")

# get log
logger = logging.getLogger(__name__)

# class & function
class Connection_Mariadb:
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DB = os.getenv("MYSQL_DB")

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

def write_to_db(ti):
    data = ti.xcom_pull(task_ids="transform_the_data_polution")
    logger.info(data)
    try:
        conf = Connection_Mariadb()
        connection = pymysql.connect(
            host=conf.MYSQL_HOST, user=conf.MYSQL_USER, password=conf.MYSQL_PASSWORD, database=conf.MYSQL_DB, port=conf.MYSQL_PORT
        )
        cursor = connection.cursor()
        query = f"""
            insert into pollution values ('{data['timestamp_utc']}',
                                            '{data['timestamp_tz']}',
                                            {data['co']},
                                            {data['h']},
                                            {data['pm10']},
                                            {data['pm25']})
        """
        logger.info(query)
        cursor.execute(query)

    except pymysql.Error as e:
        logger.error(f"Error connecting to MariaDB: {e}")
        raise Exception(f"Error connecting to MariaDB: {e}")
    finally:
        if connection:
            connection.commit()
            connection.close()
            logger.info("Connection closed")
    
def check_connection_db():
    try:
        conf = Connection_Mariadb()
        connection = pymysql.connect(
            host=conf.MYSQL_HOST, user=conf.MYSQL_USER, password=conf.MYSQL_PASSWORD, database=conf.MYSQL_DB, port=conf.MYSQL_PORT
        )
        cursor = connection.cursor()
        logger.info("Connected to MariaDB!")

    except pymysql.Error as e:
        logger.error(f"Error connecting to MariaDB: {e}")
        raise Exception(f"Error connecting to MariaDB: {e}")
    finally:
        if connection:
            connection.close()
            logger.info("Connection closed")


# start dags
default_args = {
    'owner': 'my-secound-dags',
    'start_date': days_ago(1),
}

with DAG('api_connect_mysql_pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         description='The pipeline for get AIR POLITION to DB', 
         catchup=False) as dags:
    
    check_conn_db = PythonOperator(
        task_id = 'check_conn_db',
        python_callable= check_connection_db
    )

    get_air_polution_report_today = PythonOperator(
        task_id='get_air_polution_report_today',
        python_callable=_get_air_polution_report_today
    )

    transform_the_data_polution = PythonOperator(
        task_id='transform_the_data_polution',
        python_callable=_transform_the_data_polution
    )

    write_to_db = PythonOperator(
        task_id='write_to_db',
        python_callable=write_to_db
    )

    check_conn_db >> get_air_polution_report_today >> transform_the_data_polution >> write_to_db