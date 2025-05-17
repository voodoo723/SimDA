from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pandahouse as ph
import io
import requests
import math
import matplotlib.pyplot as plt
import seaborn as sns
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#настройки графиков
sns.set(
    font_scale=1,
    style='darkgrid',
    rc={'figure.figsize':(12,5)}
        )

#функция определяет аномалии методом межквартильного размаха
def check_anomaly(df, metric, a=3, n=5):        
    curr_value = df[metric].iloc[-1]    
    df['q25'] = df[metric].shift(1).rolling(n, center=True, min_periods=1).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n, center=True, min_periods=1).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25'] #межквартильный размах
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    if curr_value > df['up'].iloc[-1] or curr_value < df['low'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'i.bondarev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 11),
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_9_1_bondarev():
    
    #выгружаем данные из кликхауса
    @task()
    def extract():        

        connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': 'simulator_20250320'
        }

        
        q_feed = """
        select toStartOfFifteenMinutes(time) as ts,
                formatDateTime(ts, '%R') as hm,
                toDate(time) as date, 
                count(distinct user_id) as feed_users,
                countIf(user_id, action = 'view') as views,
                countIf(user_id, action = 'like') as likes,          
                likes / views as ctr
        from {db}.feed_actions
        where time >= toStartOfFifteenMinutes(date_add(now(), INTERVAL -10 DAY)) and time < toStartOfFifteenMinutes(date_add(now(), INTERVAL -9 DAY))
        group by toStartOfFifteenMinutes(time), formatDateTime(ts, '%R'), toDate(time)
        order by ts desc
        """

        q_message = """
        select toStartOfFifteenMinutes(time) as ts,
                formatDateTime(ts, '%R') as hm,
                toDate(time) as date,
                count(distinct user_id) as message_users,
                count(*) as messages
        from simulator_20250320.message_actions
        where time >= toStartOfFifteenMinutes(date_add(now(), INTERVAL -10 DAY)) and time < toStartOfFifteenMinutes(date_add(now(), INTERVAL -9 DAY))
        group by toStartOfFifteenMinutes(time), formatDateTime(ts, '%R'), toDate(time)
        """

        df_feed = ph.read_clickhouse(q_feed, connection=connection)
        df_message = ph.read_clickhouse(q_message, connection=connection)
        df = pd.merge(df_feed, df_message, how='outer', on=['ts','hm','date'])
        return df
    
    #отправка алертов
    @task
    def run_alerts(df):
        
        my_token = '7436047680:AAESLKNUY8NiyNoF9vosR0BeN99X4NYaIi0'
        bot = telegram.Bot(token=my_token) # получаем доступ
        chat_id = 184589511
        
        metrics = df.columns.to_list()[3:]

        for metric in metrics:
            print(metric)
            df_alert = df[['ts','hm',metric]].copy()

            is_alert, df_alert = check_anomaly(df_alert, metric=metric, a=4, n=4)
            if is_alert or True:
                msg1='''Метрика {metric}\nТекущее значение {curr_value:.2f}\nОтклонение от предыдущего {diff:.2%}.'''.format(metric=metric, curr_value=df_alert[metric].iloc[-1], diff=abs(1 - df_alert[metric].iloc[-1]/df_alert[metric].iloc[-2]))

       
            ax = sns.lineplot(x=df_alert['ts'], y=df_alert[metric], label=metric)
            ax = sns.lineplot(x=df_alert['ts'], y=df_alert['up'], label='up', linestyle='--')
            ax = sns.lineplot(x=df_alert['ts'], y=df_alert['low'], label = 'low', linestyle='--')

            #настройка подписей времени
            min_tick = df_alert['ts'].apply(lambda x: x.timestamp()/(24* 60 *60)).min()
            max_tick = df_alert['ts'].apply(lambda x: x.timestamp()/(24* 60 *60)).max()
            new_ticks = [x for x in np.arange(min_tick,max_tick+1/12,1/12)]
            new_ticklabels = [str(timedelta(seconds=round((x-math.floor(x))*24*60*60)))[:-3] for x in np.arange(min_tick,max_tick+1/12,1/12)]

            ax.set_xticks(new_ticks, new_ticklabels)

            ax.set(xlabel='time') 
            ax.set(ylabel=metric)

            ax.set_title('{}'.format(metric))
            ax.set(ylim=(0, None))


            # формируем файловый объект
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            # отправляем алерт
            bot.sendMessage(chat_id=chat_id, text=msg1)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    df = extract()
    run_alerts(df)
    
dag_9_1_bondarev = dag_9_1_bondarev()
    