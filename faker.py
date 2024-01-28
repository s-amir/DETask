import json
import uuid
import random
from datetime import datetime, timedelta
from confluent_kafka import Producer

"""
create random event to kafka topic with three field <ad_id,timestamp,uuid>
note:to speed up test-time data inserted in batch mode
in real scenario it must has user data with uuid that's revoke each 30 minutes
number of event in view topic >= number of event in click topic
"""

# Function to create a random dictionary with ad_id, timestamp, and uuid
def create_random_dictionary():
    events = []
    now = datetime.now()
    thirty_minutes_ago = now - timedelta(minutes=30)
    uuid_list = [uuid.uuid4() for _ in range(1, 100)]

    for i in range(1, 300):
        random_seconds = random.randint(0, 30 * 60)  # 30 minutes * 60 seconds

        # Create a random timestamp within the last 30 minutes
        random_timestamp = thirty_minutes_ago + timedelta(seconds=random_seconds)
        index = random.randint(1, 99)
        print(index)
        event = {
            "ad_id": random.randint(100, 150),  # Random ad_id between 100 and 500
            "timestamp": random_timestamp.isoformat(),  # Current timestamp
            "uuid": str(uuid_list[index])
        }
        events.append(event)
    return events


conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)
events = create_random_dictionary()
count = 0
for event in events:
    if event['ad_id'] % 3 == 0 or event['ad_id'] % 5 == 0:
        producer.produce('click', json.dumps(event))
    producer.produce('view', json.dumps(event))
    print(f'done {count}')
    count += 1
for event in events:
    producer.produce('view', json.dumps(event))
    print(f'done {count}')
    count += 1
producer.flush()
