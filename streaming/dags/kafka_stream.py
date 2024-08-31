from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

log_file = 'script_log.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

default_args = {
    'owner': 'delme',
    'start_date': datetime(2024, 8, 30, 10, 00)
}


def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()['results'][0]
    return res

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time


    # ['localhost:9092'] - is you don't use docker for running - you run it manually on local machine
    # ['broker:29092'] - is you run dag from docker machine
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms = 5000)

    current_time = time.time()

    while True:
        # produce real time only for 1 minute
        if time.time() > current_time + 60: # 1minute
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error('An error occurred: {}'.format(e))
            continue

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='streaming_data_from_api',
        python_callable=stream_data
    )

# stream_data()