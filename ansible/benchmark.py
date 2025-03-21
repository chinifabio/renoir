import sys
import argparse
import time
import random
import multiprocessing
import logging

if sys.version_info >= (3, 12, 0):
    import six

    sys.modules["kafka.vendor.six.moves"] = six.moves
from kafka import KafkaProducer, KafkaConsumer

parser = argparse.ArgumentParser(description="Kafka benchmark")
parser.add_argument(
    "-s",
    "--bootstrap-servers",
    type=str,
    default="localhost:9092",
    help="Kafka bootstrap servers",
)
parser.add_argument(
    "-st", "--source-topic", type=str, default="test-source", help="Source topic"
)
parser.add_argument(
    "-dt", "--sink-topic", type=str, default="test-sink", help="Sink topic"
)
parser.add_argument(
    "-n", "--num-messages", type=int, default=1000, help="Number of messages to send"
)
args = parser.parse_args()


def produce_messages(bootstrap_servers, topic, num_messages):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    for i in range(num_messages):
        producer.send(topic, value=f"{random.randint(0, 1000)}".encode("utf-8"))
    producer.flush()
    producer.close()


def consume_messages(bootstrap_servers, topic, num_messages, start_time):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="benchmark-group",
    )
    count = 0
    for message in consumer:
        count += 1
        if count >= num_messages:
            break
    end_time = time.time()
    consumer.close()
    return end_time - start_time


start_time = time.time()

producer_process = multiprocessing.Process(
    target=produce_messages,
    args=(args.bootstrap_servers, args.source_topic, args.num_messages),
)
producer_process.start()

elapsed_time = consume_messages(
    args.bootstrap_servers, args.sink_topic, 100, start_time
)

logging.info(f"Time taken: {elapsed_time:.2f} seconds")
producer_process.join()
