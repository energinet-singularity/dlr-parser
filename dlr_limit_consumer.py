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

#file_path = '/home/thomas/Desktop/workspace/python-client-kafka/kafka-scada-parser/kafka_to_csv.csv'

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
    # buffer = []
    for message in consumer:
        print('I were here')
        message = message.value
        consumer.commit()
        return message

# Main loop
while True:
    # Start time
    start = time.time()
    print('Starting loop')
    # Receiving the last message from the kafka topic
    try:
        data = consumer_kafka_to_csv()
    except Exception as e:
        print(f"Failed to connect to kafka topic: {e}")
        time.sleep(cycle)
        continue
    
    if show_data: print(data)

    # Ensures there is a file to write to at the target location
    if not os.path.isfile(file_path):
        df = pd.DataFrame(list())
        df.to_csv(file_name)
        
    if len(data) == 0:
        continue
    else:
        try:    
            with open(file_path, 'w') as csv_file:
                title = ["I", "SEGLIM", "LINESEG_MRID", "LIMIT1", "LIMIT2", "LIMIT3"]
                w = csv.DictWriter(csv_file, delimiter = ',', fieldnames = title)
                w.writeheader()
                for i in data:
                    i[title[0]] = 'D'
                    i[title[1]] = 'SEGLIM'
                    i[title[2]] = i['mrid']
                    del i['mrid']
                    i[title[3]] = i['steady_state_rating']
                    del i['steady_state_rating']
                    i[title[4]] = i['emergency_rating_15min']
                    del i['emergency_rating_15min']
                    i[title[5]] = i['load_shedding']
                    del i['load_shedding']
                    del i['calculation_time']
                    w.writerow(i)
                
        except IOError:
            print('I/O error')

    end = time.time()
    if show_debug: print(f"Runtime of the program is {end - start}")
