from _datetime import datetime, timedelta
from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import pandas as pd
import time

def convertFile(path = "StockData"):
    files = os.listdir(path)
    os.chdir(path)
    i = 0
    for file in files:
        if file.endswith("xls") and not os.path.exists(path+"/"+file.replace("xls", "txt")):
            df = pd.read_excel(file, index_col=0)
            with open("StockData"+str(i)+".csv", "w+") as csv_file:
                pd.DataFrame(df.iloc[8:]).to_csv(csv_file, header=None)
        i+=1

def send_message(source_path = "StockData"):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: bytearray(x, 'utf-8')
    )
    files = os.listdir(source_path)
    for file in files:
        if file.endswith("csv"):
            with open(source_path+"/"+file, "r+") as csv_file:
                for line in csv_file.readlines():
                    if line != "\n":
                        line = line.replace("\n", "")
                        producer.send(topic="stock", value=line[1:])
                        time.sleep(5)

# send_message()

with DAG(
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    dag_id='pipeline',
    start_date=datetime(2022, 8, 15),
    schedule_interval='@hourly',
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id='kafka_producer_meassage',
        python_callable=send_message
    )


