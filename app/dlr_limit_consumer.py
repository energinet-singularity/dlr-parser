from kafka import KafkaConsumer
from json import loads
import time
import csv
import sys
import os

show_debug = True
show_data = True

# Input from user
IP = os.environ.get('KAFKA_IP')
topic_name = os.environ.get('KAFKA_TOPIC')
file_name = os.environ.get('FILE_NAME')
# Parameter to disable shaping of the data and instead generate a file directly from the json list
shape_data = os.environ.get('SHAPE_DATA', 'True')

# Function to call the kafka consumer and taking the last element
def consumer_kafka_to_csv():
    # buffer = []
    for message in consumer:
        print('Got message from kafka:')
        print(message.value)
        message = message.value
        consumer.commit()
        return message

def export_to_file(file_name, data, shape_data):
    if len(data) != 0:
        if shape_data:
            try:
                with open(file_name, 'w+') as csv_file:
                    # List of variables to be used as keys for the excel file
                    header = ["I", "SEGLIM", "LINESEG_MRID", "LIMIT1", "LIMIT2", "LIMIT3"]
                    w = csv.DictWriter(csv_file, delimiter = ',', fieldnames = header)
                    w.writeheader()

                    # Extract list of keys into variable
                    dlr_keys = list(data[0].keys())
                    # We iterate through the rows in data and create a new row with the correct keys. Otherwise the csv won't accept the row for further processing
                    # header[0] and header[1] are static, while header[2] through header[5] are dynamically set from the keys used in the DLR data
                    for i in data:
                        row = {header[0]: 'D',
                               header[1]: 'SEGLIM',
                               header[2]: i[dlr_keys[0]],
                               header[3]: i[dlr_keys[1]],
                               header[4]: i[dlr_keys[2]],
                               header[5]: i[dlr_keys[3]]
                               }
                        w.writerow(row)
            except IOError:
                print('I/O error')
        else:
            try:
                with open(file_name, 'w+', newline='') as csv_file:
                    csv_writer = csv.writer(csv_file)
                    data_keys = list(data[0].keys())
                    csv_writer.writerow(data_keys)

                    for i in data:
                        csv_writer.writerow(i.values())
            except IOError:
                print('I/O error')


# Main loop
if __name__ == "__main__":

    # Check to see if there is a input on the environment variables
    if IP == "":
        print('Input on IP is not set')
        sys.exit(1)

    if topic_name == "":
        print('Input on topic_name is not set')
        sys.exit(1)

    if file_name == "":
        print('Input on file_name is not set')
        sys.exit(1)

    # Wait time used if kafka connection fails
    cycle = 5

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[IP],
        auto_offset_reset='latest',
        group_id='Limit consumer',
        value_deserializer=lambda x: loads(x.decode('utf-8')))

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

        export_to_file(f'/data/{file_name}', data, shape_data)

        end = time.time()
        if show_debug: print(f"Runtime of the program is {end - start}")
