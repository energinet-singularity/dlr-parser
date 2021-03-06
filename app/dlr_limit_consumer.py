from kafka import KafkaConsumer
from os import environ as env
from json import loads
import logging
import time
import csv
import sys

# Initialize log
log = logging.getLogger(__name__)

# Input from user
try:
    ip = env.get("KAFKA_IP")
except KeyError:
    log.warning("Input on KAFKA_IP is not set")
    sys.exit(1)
try:
    topic_name = env.get("KAFKA_TOPIC")
except KeyError:
    log.warning("Input on KAFKA_TOPIC is not set")
    sys.exit(1)
try:
    file_name = env.get("FILE_NAME")
except KeyError:
    log.warning("Input on FILE_NAME is not set")
    sys.exit(1)

# Parameter to disable shaping of the data and instead generate a file directly from the json list
shape_data = env.get("SHAPE_DATA", "True")
shape_data = shape_data.upper() == "TRUE"

# Function to call the kafka consumer and taking the last element
def consumer_kafka_to_csv():
    for message in consumer:
        log.info("Got message from kafka")
        message = message.value
        log.debug(message)
        consumer.commit()
        return message


def export_to_file(file_name, data, shape_data):
    if len(data) != 0:
        if shape_data:
            try:
                with open(file_name, "w+") as csv_file:
                    # List of variables to be used as keys for the excel file
                    header = [
                        "I",
                        "SEGLIM",
                        "LINESEG_MRID",
                        "LIMIT1",
                        "LIMIT2",
                        "LIMIT3",
                    ]
                    w = csv.DictWriter(csv_file, delimiter=",", fieldnames=header)
                    w.writeheader()

                    # Extract list of keys into variable
                    dlr_keys = list(data[0].keys())
                    # We iterate through the rows in data and create a new row with the correct keys. Otherwise the csv won't accept the row for further processing
                    # header[0] and header[1] are static, while header[2] through header[5] are dynamically set from the keys used in the DLR data
                    for i in data:
                        row = {
                            header[0]: "D",
                            header[1]: "SEGLIM",
                            header[2]: i[dlr_keys[0]].replace("-", ""),
                            header[3]: i[dlr_keys[1]],
                            header[4]: i[dlr_keys[2]],
                            header[5]: i[dlr_keys[3]],
                        }
                        w.writerow(row)
            except IOError:
                log.error("I/O error")
        else:
            try:
                with open(file_name, "w+", newline="") as csv_file:
                    csv_writer = csv.writer(csv_file)
                    data_keys = list(data[0].keys())
                    csv_writer.writerow(data_keys)

                    for i in data:
                        csv_writer.writerow(i.values())
            except IOError:
                log.error("I/O error")


# Main loop
if __name__ == "__main__":

    # Setup logging for client output (__main__ should output INFO-level, everything else stays at WARNING)
    logging.basicConfig(format="%(levelname)s:%(asctime)s:%(name)s - %(message)s")
    logging.getLogger(__name__).setLevel(logging.INFO)

    # Wait time used if kafka connection fails
    cycle = 5

    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=ip,
            auto_offset_reset="latest",
            group_id="Limit consumer",
            value_deserializer=lambda x: loads(x.decode("utf-8")),
        )
    except Exception:
        log.exception(
            "Connection to kafka failed. Check environment variable KAFKA_IP and reload the script/container to try again."
        )

    while True:

        log.info("Starting loop")
        # Receiving the last message from the kafka topic
        try:
            data = consumer_kafka_to_csv()
        except Exception as e:
            log.warning(f"Failed to connect to kafka topic: {e}")
            time.sleep(cycle)
            continue

        log.info(data)

        # Start time of the internal part of the program
        start = time.time()
        export_to_file(f"/data/{file_name}", data, shape_data)
        end = time.time()

        log.debug(f"Runtime of the program is {end - start}")
