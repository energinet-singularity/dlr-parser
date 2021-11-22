from kafka import KafkaConsumer
from json import loads
import time
import pandas as pd
import csv
import os


IP = os.environ['KAFKA_IP']
topic_name = os.environ['KAFKA_TOPIC']
file_path = os.environ['FILE_PATH']
file_name = os.environ['FILE_NAME']

# file_path = '/home/thomas/Desktop/workspace/python-client-kafka/kafka-scada-parser/kafka_to_csv.csv'

cycle = 2
show_debug = True
show_data = True

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[IP],
    auto_offset_reset='latest',
    group_id='Limit consumer',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

def consumer_kafka_to_csv():
    for i,message in enumerate(consumer):
        if i < 1:
            message = message.value
            #print('{} added'.format(message))
            consumer.commit()
        else:
            break
        return message

while True:

    start = time.time()
    try:
        data = consumer_kafka_to_csv()
    except:
        print('Failed to connect to kafka topic')
        time.sleep(cycle)
        continue
    
    if show_data: print(data)

    if not os.path.isfile(file_path):
        df = pd.DataFrame(list())
        df.to_csv(file_name)
    

    if True:
        try:
            with open(file_path, 'w') as csv_file:
                w = csv.DictWriter(csv_file, fieldnames = data.keys())
                w.writeheader()
                for i in data:
                    w.writerow(data)
        except IOError:
            print('I/O error')
    else:
        print('No new data in kafka topic')

    end = time.time()
    if show_debug: print(f"Runtime of the program is {end - start}")
    