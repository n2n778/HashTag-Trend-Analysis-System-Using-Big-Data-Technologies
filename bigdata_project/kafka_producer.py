from kafka import KafkaProducer
import json, time, csv, os

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

dataset_path = os.path.join(os.path.dirname(__file__), 'tweets_dataset.csv')

print("Reading dataset and sending to Kafka...")

while True:                              # loops the dataset continuously
    with open(dataset_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            msg = {
                "user":      row["user"],
                "text":      row["text"],
                "timestamp": row["timestamp"]
            }
            producer.send('twitter_stream', msg)
            print(f"Sent → {row['user']}: {row['text']}")
            time.sleep(0.3)              # sends 1 tweet every 0.3 sec
    print("Dataset loop complete — restarting...")
    time.sleep(2)

