# from datetime import datetime
# from airflow import DAG
# import uuid
# from airflow.operators.python import PythonOperator

# default_args = {
#     'owner' : 'airscholar',
#     'start_date' : datetime(2023, 9, 3, 10, 00)
    
# }

# def get_data():
  
#     import requests
    
#     res = requests.get("https://randomuser.me/api/")
#     res = (res.json())
#     res = res['results'][0]
    
#     return res

# def format_data(res):
#     data = {}
#     location = res['location']
#     #data['id'] = uuid.uuid4()
#     data['first_name'] = res['name']['first']
#     data['last_name'] = res['name']['last']
#     data['gender'] = res['gender']
#     data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
#                       f"{location['city']}, {location['state']}, {location['country']}"
#     data['post_code'] = location['postcode']
#     data['email'] = res['email']
#     data['username'] = res['login']['username']
#     data['dob'] = res['dob']['date']
#     data['registered_date'] = res['registered']['date']
#     data['phone'] = res['phone']
#     data['picture'] = res['picture']['medium']

#     return data
     
# def stream_data():
#     import json
#     from kafka import KafkaProducer
#     import time
#     import logging
#     import requests
    
   
#     producer = KafkaProducer(bootstrap_servers=['broker:9092'], max_block_ms=5000)
#     curr_time = time.time()
    
#     while True:
#         if time.time() > curr_time + 200:
#             break
#         try:
#             res = get_data()
#             res = format_data(res)
            
#             producer.send('users_created', json.dumps(res).encode('utf-8'))    
#         except Exception as e:
#             logging.error(f'An error occured: {e}')
#             continue 
    
# with DAG('user_automation',
#          default_args=default_args,
#          schedule='@daily',
#          catchup=False) as dag:

#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )

######################################################################################code that works with airflow and using AG sends to kafka control center###################################


# from datetime import datetime
# from airflow import DAG
# import uuid
# from airflow.operators.python import PythonOperator

# default_args = {
#     'owner' : 'airscholar',
#     'start_date' : datetime(2023, 9, 3, 10, 00)
    
# }

# def get_data():
  
#     import requests
    
#     res = requests.get("https://randomuser.me/api/")
#     res = (res.json())
#     res = res['results'][0]
    
#     return res

# def format_data(res):
#     data = {}
#     location = res['location']
#     data['id'] = str(uuid.uuid4()) 
#     data['first_name'] = res['name']['first']
#     data['last_name'] = res['name']['last']
#     data['gender'] = res['gender']
#     data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
#                       f"{location['city']}, {location['state']}, {location['country']}"
#     data['post_code'] = location['postcode']
#     data['email'] = res['email']
#     data['username'] = res['login']['username']
#     data['dob'] = res['dob']['date']
#     data['registered_date'] = res['registered']['date']
#     data['phone'] = res['phone']
#     data['picture'] = res['picture']['medium']

#     return data
     
# def stream_data():
#     import json
#     from kafka import KafkaProducer
#     import time
#     import logging
#     import requests
    
   
#     producer = KafkaProducer(bootstrap_servers=['broker:9092'], max_block_ms=5000)
#     curr_time = time.time()
    
#     while True:
#         if time.time() > curr_time + 200:
#             break
#         try:
#             res = get_data()
#             res = format_data(res)
            
#             producer.send('users_created', json.dumps(res).encode('utf-8'))    
#         except Exception as e:
#             logging.error(f'An error occured: {e}')
#             continue 
    
# with DAG('user_automation',
#          default_args=default_args,
#          schedule='@daily',
#          catchup=False) as dag:

#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )
########################################################that works with spark and cassandra
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer
import json
import logging
import time

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 0)
}

# 🔥 Fire risk evaluation logic
def evaluate_fire_risk(data):
    temperature = data["temperature"]
    humidity = data["humidity"]
    wind_speed = data["wind_speed"]
    soil_moisture = data["soil_moisture"]

    if temperature > 35 and humidity < 30 and wind_speed > 20 and soil_moisture < 20:
        return "HIGH 🔥"
    elif temperature > 30:
        return "MEDIUM ⚠️"
    else:
        return "LOW ✅"

# ✅ Kafka Consumer + Fire Risk Processing
def consume_fire_events(duration_seconds: int = 60):
    try:
        consumer = KafkaConsumer(
            'fire_sensors',
            bootstrap_servers=['broker:9092'],  # Docker Kafka broker
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='fire_consumer_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        start_time = datetime.now().timestamp()
        count = 0

        for message in consumer:
            data = message.value
            risk_status = evaluate_fire_risk(data)
            count += 1

            # Print nicely for demo
            if count % 20 == 0:
                print(f"Processed {count} events. Latest sensor {data['sensor_id']} → {risk_status}")
            
            # Optional: short sleep to prevent DB or console overload
            time.sleep(0.01)

            if datetime.now().timestamp() > start_time + duration_seconds:
                break

    except Exception as e:
        logging.error(f"Error consuming Kafka events: {e}")

# 🔥 Airflow DAG
with DAG(
    'fire_sensor_streaming',
    default_args=default_args,
    schedule=None,  # manual trigger for demo
    catchup=False
) as dag:

    consume_task = PythonOperator(
        task_id='consume_fire_sensor_events',
        python_callable=consume_fire_events
    )