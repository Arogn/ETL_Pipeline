# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


#словарь-коннектор к БД-источнику для сбора данных (удалены так как база данных приватна) 
connection = {
    'host': '',
    'password': '',
    'user': '',
    'database': ''
}

#словарь-коннектор к целевому БД для загрузки данных (удалены так как база данных приватна) 
connection2 = {
    'host': '',
    'password': '',
    'user': '',
    'database': ''
}


#параметры, которые прокидываются в таски 
default_args = {
    'owner': 'arogn',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 22),
}

#cron-выражение, говорящее о том, что надо запускать DAG в 23:00
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_arogn():

    #таск для извлечения количества лайков и просмотров на каждого пользователя за вчерашний день
    @task()
    def extract_feed():
        query = """ SELECT
                         toDate(time) as event_date
                       , gender
                       , age 
                       , os
                       , user_id as user
                       , countIf(action = 'view') as views
                       , countIf(action = 'like') as likes 
                    FROM tables.f_a
                    WHERE toDate(time) = yesterday() 
                    GROUP BY event_date, gender, age, os, user
        """        
        df_cube = pandahouse.read_clickhouse(query, connection=connection)
        return df_cube
    
    #таск для извлечения для каждого пользователя количества полученных и отосланных сообщений, 
    #количества людей, которым он написал и которые ему написали
    @task()
    def extract_messages():
        query = """ SELECT 
                        event_date
                      , gender
                      , age
                      , user
                      , os
                      , messages_sent
                      , messages_recieved
                      , users_sent
                      , users_recieved  
                    FROM (
                    SELECT 
                        toDate(time) as event_date
                      , gender
                      , age
                      , os
                      , user_id as user
                      , count() as messages_sent 
                      , uniq(reciever_id) as users_sent 
                    FROM tables.m_a
                    WHERE toDate(time) = yesterday() 
                    GROUP BY event_date, gender, os, age, user) as sent 
                    full outer join 
                    (SELECT 
                        toDate(time) as event_date
                      , gender
                      , age
                      , os
                      , reciever_id as user
                      , count() as messages_recieved
                      , uniq(user_id) as users_recieved
                    FROM tables.m_a
                    WHERE toDate(time) = yesterday() 
                    GROUP BY event_date, gender, age, os, user) as recieve 
                    using user
        """        
        df_cube = pandahouse.read_clickhouse(query, connection=connection)
        return df_cube

    #таск для объединения извлеченных таблиц
    @task
    def join_cubes(messages,feeds):
        final_cube = messages.merge(feeds, how='outer', on=['user', 'event_date', 'gender', 'age', 'os']).dropna()
        return final_cube

    #группировка объединенной таблицу по полу, возрасту и ос
    @task
    def transfrom_metric(df_cube, one_metric):
        full_metric_list = ['event_date', 'views', 'likes', 'messages_recieved', 'messages_sent', 'users_recieved',  'users_sent'] + [one_metric]
        group_metric_list = ['event_date'] + [one_metric]
        transform_cube = df_cube[full_metric_list].groupby(group_metric_list).sum().reset_index()
        transform_cube.rename(columns = {str(one_metric):'dimension_value'}, inplace = True)
        transform_cube["dimension"] = str(one_metric)
        return transform_cube
    
    #загрузка полученных сгруппированных таблиц в таблицу arogn_table
    @task
    def load(gender, age, os):
        print('os load')
        pandahouse.to_clickhouse(os, table='arogn_table', connection=connection2, index=False)
        print('age load')
        pandahouse.to_clickhouse(age, table='arogn_table', connection=connection2, index=False)
        print('gender load')
        pandahouse.to_clickhouse(gender, table='arogn_table', connection=connection2, index=False)

    df_cube_feed = extract_feed()
    df_cube_messages = extract_messages()
    full_cube = join_cubes(df_cube_feed,df_cube_messages)
    
    gender = transfrom_metric(full_cube, 'gender')
    age = transfrom_metric(full_cube, 'age')
    os = transfrom_metric(full_cube, 'os')

    load(gender, age, os)

dag_arogn = dag_arogn()