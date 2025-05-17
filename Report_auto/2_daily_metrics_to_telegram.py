import telegram
import numpy as np
import pandahouse as ph
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'i.bondarev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 8, 21),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_8_11_bondarev():
        
    @task()
    def extract():        
        #подключаемся к кликхаусу
        connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': 'simulator_20250320'
        }
        
        #вытащим значения метрик за последние 7 дней
        q = """
        select toDate(time) as date, 
                count(distinct user_id) as dau,
                countIf(user_id, action = 'view') as views,
                countIf(user_id, action = 'like') as likes,          
                likes / views as ctr
        from {db}.feed_actions
        where toDate(time) between yesterday()-6 and yesterday()
        group by toDate(time)
        """

        df = ph.read_clickhouse(q, connection=connection)
        df = df.assign(dt = df.date.dt.strftime('%d.%m'))
        return df
    
    @task
    def load(df):
    
        my_token = '7436047680:AAESLKNUY8NiyNoF9vosR0BeN99X4NYaIi0'
        bot = telegram.Bot(token=my_token) # получаем доступ

        #настройки графиков
        sns.set(
            font_scale=1,
            style="darkgrid",
            rc={'figure.figsize':(12,5)}
                )

        chat_id = -938659451
        chat_id_old = 184589511

        #значения метрик за вчера
        metrics_list = df.sort_values(by = 'date', ascending = False).head(1).iloc[0].tolist()

        #формируем первое сообщение с метриками за вчера
        msg1 = 'Метрики за {dt}:\n\n-DAU: {dau}\n-Просмотры: {views}\n-Лайки: {likes}\n-CTR: {ctr}'\
            .format(dt = metrics_list[0].date().strftime('%d.%m.%Y'),
                    dau = metrics_list[1],
                    views = metrics_list[2],
                    likes = metrics_list[3],
                    ctr = f"{metrics_list[4]:.1%}" 
                   )
        
        #отправляем сводку за вчера
        bot.sendMessage(chat_id=chat_id, text=msg1)

        #график DAU
        sns.lineplot(data = df, x = 'dt', y = 'dau', color='r', linewidth = 3, marker = 'o', markersize = 10)
        plt.title('DAU')
        plot_object_dau = io.BytesIO()
        plt.savefig(plot_object_dau)
        plot_object_dau.seek(0)
        plot_object_dau.name = 'DAU.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object_dau)

        #график просмотров
        sns.lineplot(data = df, x = 'dt', y = 'views', color='g', linewidth = 3, marker = 'o', markersize = 10)
        plt.title('Просмотры')
        plot_object_views = io.BytesIO()
        plt.savefig(plot_object_views)
        plot_object_views.seek(0)
        plot_object_views.name = 'Views.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object_views)

        #график лайков
        sns.lineplot(data = df, x = 'dt', y = 'likes', color='b', linewidth = 3, marker = 'o', markersize = 10)
        plt.title('Лайки')
        plot_object_likes = io.BytesIO()
        plt.savefig(plot_object_likes)
        plot_object_likes.seek(0)
        plot_object_likes.name = 'Likes.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object_likes)

        #график CTR
        sns.lineplot(data = df, x = 'dt', y = 'ctr', color='violet', linewidth = 3, marker = 'o', markersize = 10)
        plt.title('CTR')
        plot_object_ctr = io.BytesIO()
        plt.savefig(plot_object_ctr)
        plot_object_ctr.seek(0)
        plot_object_ctr.name = 'CTR.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object_ctr)
        
    df = extract()
    load(df)
    
dag_8_11_bondarev = dag_8_11_bondarev()
