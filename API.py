import datetime
import http.client
import json
import time
import models
from confluent_kafka import Producer

"""API class note:It's more reliable to store data in Redis or memory-cache and also functions must be more 
independent with each other"""


class API_stream():
    def __init__(self, previous_page_event_id, topic_name):
        self.previous_page_event_id = previous_page_event_id
        self.topic_name = topic_name

    def get_response_object(self):
        conn = ''
        try:
            conn = http.client.HTTPSConnection("bama.ir")
            payload = ''
            headers = {}
            conn.request("GET", "/cad/api/search?pageIndex=1", payload, headers)
            res = conn.getresponse()
            response_content = res.read()
            return json.loads(response_content.decode('utf-8'))
        except Exception as e:
            print(e)
        finally:
            if isinstance(conn, http.client.HTTPSConnection):
                conn.close()

    def check_insert_event(self):
        events = self.get_response_object()['data']['ads']
        current_page_event_id = []
        event_list_to_kafka = []

        for event in events:
            if event.get('type') == 'banner':
                continue
            event_id = event['detail']['code']
            current_page_event_id.append(event_id)
            if event_id not in self.previous_page_event_id:
                model_instance = models.EventData(event)
                model_instance_dict = model_instance.to_dict()
                event_list_to_kafka.append(model_instance_dict)

        self.previous_page_event_id = current_page_event_id
        return event_list_to_kafka

    def kafka_producer_creator(self):
        conf = {'bootstrap.servers': 'localhost:9092'}
        producer = Producer(conf)
        return producer

    def produce_kafka_event(self, producer: Producer, event):
        try:
            if event:
                event = json.dumps(event)
                producer.produce(self.topic_name, key=str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")),
                                 value=event)
                producer.flush()
            else:
                print('event is null')
        except Exception as e:
            print(e)
            return False
        print(f'{event} is produced')
        return True

    def loop_API_fetch(self):
        producer = self.kafka_producer_creator()
        status = True
        while True:
            if not status:
                producer = self.kafka_producer_creator()
            event_list = self.check_insert_event()
            status = self.produce_kafka_event(producer, event_list)
            time.sleep(20)


a = API_stream([], 'bama')
a.loop_API_fetch()
