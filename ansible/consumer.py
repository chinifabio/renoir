import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaConsumer

# Configura il Consumer Kafka
consumer = KafkaConsumer(
    'output',  # Sostituisci con il nome del topic
    bootstrap_servers='localhost:9093',  # Cambia con l'host e la porta di Kafka
    auto_offset_reset='earliest',  # Legge dall'inizio del topic se non ci sono offset salvati
    group_id='my_consumer_group',  # Nome del gruppo per gestire più Consumer
    enable_auto_commit=True  # Imposta True per confermare automaticamente la lettura
)

print("Consumer in ascolto...")

try:
    for message in consumer:
        # Decodifica il valore del messaggio
        number = message.value.decode('utf-8')
        print(f"Ricevuto numero: {number}")

except KeyboardInterrupt:
    print("Consumer interrotto manualmente.")
finally:
    # Chiudi il Consumer in modo sicuro
    consumer.close()

