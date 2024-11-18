import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import time
import random
from kafka import KafkaProducer

if len(sys.argv) != 3:
    print("Usage: python producer.py <target_topic> <sleep_time>")
    sys.exit(1)
TARGET_TOPIC = sys.argv[1]
SLEEP_TIME = sys.argv[2]

# Configura il Producer Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9093')  # Cambia 'localhost:9092' con l'host e la porta di Kafka
topic = TARGET_TOPIC

try:
    while True:
        # Genera un numero casuale tra 1 e 100
        random_number = random.randint(1, 100)

        # Invia il numero al topic Kafka
        producer.send(topic, value=f"{{\"Item\":{random_number}}}".encode('utf-8'))

        # Stampa per conferma
        print(f"Inviato numero: {random_number}")

        # Aspetta 100 millisecondi (0.1 secondi)
        time.sleep(float(SLEEP_TIME))

except KeyboardInterrupt:
    print("Interrotto manualmente.")
finally:
    # Chiudi il Producer Kafka in modo sicuro
    producer.close()

