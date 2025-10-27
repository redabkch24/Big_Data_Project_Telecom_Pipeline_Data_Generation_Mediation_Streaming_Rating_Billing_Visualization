from kafka import KafkaProducer
from generate_record import generate_record
import json
import time

# Configurer le producer Kafka
kafka_topic = "raw-cdrs"
kafka_bootstrap_servers = "localhost:9092"


producer = KafkaProducer(
     bootstrap_servers=[kafka_bootstrap_servers],
     value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


# number of records that we want generate
number_of_records = 10000

for i in range(number_of_records):
    records = generate_record()
    for record in records:
        producer.send(kafka_topic, record)
        print(f"record {i+1}")
        time.sleep(0.01)

# S'assurer que tous les messages sont envoyés
producer.flush()

print(f"{number_of_records} enregistrements publiés dans le topic Kafka '{kafka_topic}'.")
