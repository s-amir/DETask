import ast
import sys
import time

import clickhouse_connect
from clickhouse_connect.driver.httpclient import HttpClient
from confluent_kafka import Consumer, KafkaError, KafkaException


def create_clickhouse_client():
    client = clickhouse_connect.get_client(host='localhost', port=8123)
    return client


def prepare_clickhouse_record(events: list):
    # get keys
    keys = list(events[0].keys())
    values = [[event.get(key) for key in keys] for event in events]
    return values, keys


def insert_batch_clickhouse(list_of_values: list, list_of_keys: list, client, table_name):
    print(list_of_keys)
    print(list_of_values)
    client.insert(table=table_name, data=list_of_values, column_names=list_of_keys, column_type_names=['String','String','String','String','String','String','String'])

    return True


def creat_consumer(group_name):
    conf = {'bootstrap.servers': 'localhost:9092',
            f'group.id': group_name,
            'auto.offset.reset': 'latest'}

    consumer = Consumer(conf)
    return consumer


def consume(consumer: Consumer, topic_name):
    consumer.subscribe([topic_name, ])
    while True:
        print('consuming...')
        msg = consumer.poll()
        if msg is None:
            print('none')
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            msg = ast.literal_eval(msg.value().decode('utf-8'))
            return msg

def consume_insert_loop():
    kafka_consumer=creat_consumer('bama1')
    client=create_clickhouse_client()
    while True:
        print('start new loop!')
        if not kafka_consumer:
            kafka_consumer = creat_consumer('bama1')
        if not client:
            client = create_clickhouse_client()
        msg=consume(kafka_consumer,'bama')
        values,keys=prepare_clickhouse_record(msg)
        status=insert_batch_clickhouse(values,keys,client,'bama_ads')
        if status:
            print('inserted')
            print(msg)
        time.sleep(20)


consume_insert_loop()

# consumer = creat_consumer('bama')
# consume(consumer, 'bama')
