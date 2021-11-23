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

cycle = 5
show_debug = True
show_data = True

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[IP],
    auto_offset_reset='latest',
    group_id='Limit consumer',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

# Function to call the kafka consumer and taking the last element
def consumer_kafka_to_csv():
    buffer = []
    for message in consumer:
        time = message.value.get('calculation_time')
        if len(buffer) > 0:
            if buffer[0].get('calculation_time') != time:
                buffer = []
        buffer.append(message.value)

        if len(buffer) == 6:
            return buffer


# Main loop
while True:
    # Start time
    start = time.time()
    if show_debug: print('Starting loop')
    # Receiving the last message from the kafka topic
    try:
        data = consumer_kafka_to_csv()
    except Exception as e:
        print(f"Failed to connect to kafka topic: {e}")
        time.sleep(cycle)
        continue
    
    # For debugging
    if show_data: print(data)

    # Ensures there is a file to write to at the target location
    if not os.path.isfile(file_path):
        df = pd.DataFrame(list())
        df.to_csv(file_name)

    try:
        with open(file_path, 'w') as csv_file:
            w = csv.DictWriter(csv_file, fieldnames = data[0].keys())
            w.writeheader()
            for i in data:
                w.writerow(i)
    except IOError:
        print('I/O error')

    end = time.time()
    if show_debug: print(f"Runtime of the program is {end - start}")
