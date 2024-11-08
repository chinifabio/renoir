import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import time
import random
from kafka import KafkaProducer

# Configura il Producer Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9093')  # Cambia 'localhost:9092' con l'host e la porta di Kafka
topic = 'input'  # Sostituisci con il tuo topic

try:
    while True:
        # Genera un numero casuale tra 1 e 100
        random_number = random.randint(1, 100)

        # Invia il numero al topic Kafka
        producer.send(topic, value=str(random_number).encode('utf-8'))

        # Stampa per conferma
        print(f"Inviato numero: {random_number}")

        # Aspetta 100 millisecondi (0.1 secondi)
        time.sleep(0.1)

except KeyboardInterrupt:
    print("Interrotto manualmente.")
finally:
    # Chiudi il Producer Kafka in modo sicuro
    producer.close()

