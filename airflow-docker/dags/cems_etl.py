from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
import configparser
import requests
import pandas as pd
import json
from pandas import json_normalize
from sqlalchemy import create_engine
import pymysql
from datetime import datetime, timedelta


default_args = {
    'owner': 'shinn',
    'start_date': datetime(2023, 1, 14),
    'schedule_interval': '@daily'
}

path = '/opt/airflow/dags/conn.cfg'
config = configparser.ConfigParser()
config.read(path)
db_info = config["CEMS_DB"]
key_info = config["API_KEY"]

def get_cems(snap_date):
    df = pd.DataFrame()
    date = snap_date
    for i in range(0,40000,1000):
        r=requests.get(f'https://data.epa.gov.tw/api/v2/aqx_p_187?format=json&offset={i}&limit=1000&api_key={key_info["cems"]}&filters=m_time,GR,{date} 00:00:00|m_time,LE,{date} 23:59:59')
        info = json.loads(r.text)
        if info['records'] == []:
            break
        temp = json_normalize(info['records'])
        df = pd.concat([df,temp], ignore_index=True)
    print('origin data rows: ', len(df))
    # change column names
    df.columns=['epb', 'cno', 'abbr', 'polno', 'itemdesc', 'item','m_time', 'm_val', 'std', 'unit', 'code2', 'std_s','code2desc']
    # drop null emission value
    df = df.drop(df.loc[df['m_val']==''].index)
    # drop wrong value
    pattern = r'[0-9]{4}-[0-9]{2}-[0-9]{2}'
    df = df.drop(df.loc[df['m_val'].str.contains(pattern) == True].index)
    # standardize format & drop duplicates
    df['cno']=df['cno'].str.upper()
    df['polno']=df['polno'].str.upper()
    df['m_time']=pd.to_datetime(df['m_time'], format='%Y-%m-%d %H:%M:%S')
    column_names = ['cno', 'polno', 'm_time','item']
    df.drop_duplicates(subset=column_names, keep='first', inplace=True)
    # insert data
    sqlEngine = create_engine(f'mysql+pymysql://{db_info["user"]}:{db_info["pass"]}@{db_info["host"]}:3306/{db_info["schema"]}')
    print('insert data rows: ',len(df))
    df.to_sql('ods_cems',sqlEngine,if_exists='append',index = False)
    sqlEngine.dispose()

with DAG('CEMS_ETL', default_args=default_args) as dag:
    get_cems = PythonOperator(
        task_id='get_cems',
        python_callable=get_cems,
        op_args=['{{ ds }}']
    )
    mysql_etl = MySqlOperator(
        mysql_conn_id='cems', 
        task_id='mysql_etl',
        sql='./mysql_etl.sql')

    get_cems >> mysql_etl
