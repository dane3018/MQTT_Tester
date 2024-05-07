import time
from paho.mqtt import client as mqtt_client

timeout_value = 60


class Publisher:
    def __init__(self, client: mqtt_client.Client, name, id):
        self.client = client
        self.name = name
        self.id = id
        self.qos = None
        self.delay = None
        self.instancecount = None

        pass
    


    def publish_loop(self, topic):
        counter = 0
        start_time = time.time()
        while timeout_value - start_time > 0:
            self.client.publish(topic=topic, payload=counter, qos=self.qos)
            counter += 1

