from confluent_kafka import Consumer
import re

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'cleaning-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

topic = 'books'
consumer.subscribe([topic])

output_file = 'cleaned_book.txt'

with open(output_file, 'w', encoding='utf-8') as file:

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        text = msg.value().decode('utf-8')

        # Basic text cleaning
        text = text.lower()
        text = re.sub(r'[^a-z\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()

        if text:
            file.write(text + '\n')
            print(text)

consumer.close()