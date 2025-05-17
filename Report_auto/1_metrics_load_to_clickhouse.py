# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Функция для CH
def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'i.bondarev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 8, 17),
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_7_1_bondarev():
    # Вытащим данные по ленте за вчера
    @task()
    def extract_feed():
        query = """select toDate(time) as event_date, user_id,  os, gender, age, countIf(user_id, action = 'like') as likes,  countIf(user_id, action = 'view') as views
                    from simulator_20250320.feed_actions
                    where toDate(time) = yesterday()
                    group by toDate(time), user_id,  os, gender, age
                    format TSVWithNames"""
        df_feed = ch_get_df(query=query)
        return df_feed
    # Вытащим данные по мессенджеру за вчера
    @task()
    def extract_message():
        query = """select t1.*, t2.messages_received, t2.users_received
                    from (
                        select toDate(time) as event_date, user_id, os, gender, age, count(user_id) as messages_sent, count(distinct receiver_id) as users_sent
                        from simulator_20250320.message_actions
                        where toDate(time) = yesterday()
                        group by toDate(time), user_id, os, gender, age) t1

                        full join
                        (
                        select toDate(time) as event_date, receiver_id as user_id, count(receiver_id) as messages_received, count(distinct user_id) as users_received
                        from simulator_20250320.message_actions
                        where toDate(time) = yesterday()
                        group by toDate(time), receiver_id
                        )t2 using user_id
                        where event_date = yesterday()
                    format TSVWithNames"""
        df_message = ch_get_df(query=query)
        return df_message

    
    
    # Объединяем данные по ленте и мессенджеру
    @task
    def transfrom_union(df_feed, df_message):
        df_united = df_feed.merge(df_message, how = 'outer').fillna(0)\
                       .astype({'views' : 'int',
                                  'likes' : 'int',
                                  'messages_received' : 'int',
                                  'messages_sent' : 'int',
                                  'users_received' : 'int',
                                  'users_sent' : 'int'})
        return df_united

    # Группируем по ОС
    @task
    def transfrom_os(df_united):
        df_os = df_united.groupby(['event_date','os'], as_index = False)\
                        .agg({'views':'sum', 
                              'likes':'sum', 
                              'messages_received':'sum', 
                              'messages_sent':'sum', 
                              'users_received':'sum', 
                              'users_sent':'sum'})\
                        .rename(columns = {'os':'dimension_value'})
        df_os.insert(1, 'dimension', 'os')
        return df_os
    # Группируем по полу
    @task
    def transfrom_gender(df_united):
        df_gender = df_united.groupby(['event_date','gender'], as_index = False)\
                        .agg({'views':'sum', 
                              'likes':'sum', 
                              'messages_received':'sum', 
                              'messages_sent':'sum', 
                              'users_received':'sum', 
                              'users_sent':'sum'})\
                        .astype({'gender' : 'str'})\
                        .rename(columns = {'gender':'dimension_value'})
        df_gender.insert(1, 'dimension', 'gender')
        return df_gender
    # Группируем по возрасту
    @task
    def transfrom_age(df_united):
        df_age = df_united.groupby(['event_date','age'], as_index = False)\
                        .agg({'views':'sum', 
                              'likes':'sum', 
                              'messages_received':'sum', 
                              'messages_sent':'sum', 
                              'users_received':'sum', 
                              'users_sent':'sum'})\
                        .rename(columns = {'age':'dimension_value'})
        df_age.insert(1, 'dimension', 'age')
        return df_age
    # Загружаем в кликхаус
    @task
    def load(df_os, df_gender, df_age):
        connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                  'database':'test',
                  'user':'student-rw', 
                  'password':'656e2b0c9c'
                 }

        query_test = '''CREATE TABLE IF NOT EXISTS test.bondarev_daily_metrics
                        (event_date Date,
                         dimension String,
                         dimension_value String,
                         views UInt32 ,
                         likes UInt32 ,
                         messages_received UInt32 ,
                         messages_sent UInt32 ,
                         users_received UInt32 ,
                         users_sent UInt32 
                        )
                        ENGINE = MergeTree()
                        ORDER BY event_date
                    '''
        ph.execute(query_test, connection=connection_test)
        ph.to_clickhouse(df=df_os, table="bondarev_daily_metrics", index=False, connection=connection_test)
        ph.to_clickhouse(df=df_gender, table="bondarev_daily_metrics", index=False, connection=connection_test)
        ph.to_clickhouse(df=df_age, table="bondarev_daily_metrics", index=False, connection=connection_test)
        

    df_feed = extract_feed()
    df_message = extract_message()
    df_united = transfrom_union(df_feed, df_message)
    df_os = transfrom_os(df_united)
    df_gender = transfrom_gender(df_united)
    df_age = transfrom_age(df_united)
    load(df_os, df_gender, df_age)
    
dag_7_1_bondarev = dag_7_1_bondarev()
