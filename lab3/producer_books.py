# %%
import socket
import time
from confluent_kafka import Producer
from datetime import datetime, timedelta


# %%
print(datetime.now().strftime("%H:%M"))
# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

# %%
topic='books'
filename = 'pg11.txt'

with open(filename, 'r', encoding='utf-8') as file:
    for line in file:
        line = line.strip()

        if line:
            producer.produce(
                topic=topic,
                value=line
            )

            print(line)
            time.sleep(0.01)

producer.flush()
producer.close()
