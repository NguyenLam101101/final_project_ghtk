from _datetime import datetime, timedelta
from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import pandas as pd
import time
from PowerBI.sendDatatoPB import postToPowerBI


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
    with open("KafkaProducer/markedLine.txt", "r") as mlFile:
        marked_line = int(mlFile.read())
    for file in files:
        if file.endswith("csv"):
            i = 0
            with open(source_path+"/"+file, "r") as csv_file:
                while(i<=marked_line):
                    line = csv_file.readline()
                    i+=1
                while(line == "\n"):
                    line = csv_file.readline()
                    i+=1
                line = line.replace("\n", "")
                if line!=None:
                    producer.send(topic="stock", value=line[1:])
            csv_file.close()
    with open("KafkaProducer/markedLine.txt", "w") as mlFile:
        mlFile.write(str(i))
    time.sleep(5)


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
     schedule_interval=timedelta(seconds=30),
     catchup=False,
     ) as dag:
         t1 = PythonOperator(
             task_id='kafka_producer_message',
             python_callable=send_message
         )
         t2 = PythonOperator(
             task_id='post_to_PowerBI',
             python_callable=postToPowerBI
         )
         t1 >> t2

