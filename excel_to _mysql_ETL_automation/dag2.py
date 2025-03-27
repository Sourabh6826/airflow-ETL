import pymysql
import mysql.connector
import pandas as pd
from datetime import datetime,timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

def excel_to_mysql():
 df = pd.read_csv('FIFA.csv')

 mysql_config = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = 'root',
        database = 'PROJECT'
 )
 mycrsr = mysql_config.cursor ()
 mycrsr.execute('CREATE TABLE [IF NOT EXISTS] FIFA(date datetime, home_team varchar(255),away_team varchar(255), home_score int, away_score int, tournament varchar(255), city varchar(255), country varchar(255), neutral varchar(255)')                                    
 sql = "INSERT INTO FIFA (date, home_team, away_team,home_score,away_score,tournament,city,country,neutral ) VALUES (%s, %s, ...)"
 for index, row in df.iterrows():
    val = tuple(row)  # Convert DataFrame row to a tuple
    mycursor.execute(sql, val)
 mysql_config.commit()
 mysql_config.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'mysql_etl_dag2',  # DAG name
    default_args=default_args,
    description='A simple ETL DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 21),
    catchup=False,
)

run_etl = PythonOperator(
    task_id='run_etl',
    python_callable= excel_to_mysql ,
    dag=dag,
)   
 