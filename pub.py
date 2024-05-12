# python 3.11

import random
import time
from publisher import Publisher 

from paho.mqtt import client as mqtt_client


broker = 'localhost'
port = 1883
topic = "python/mqtt"
# Generate a Client ID with the publish prefix.
client_id = f'publish-{random.randint(0, 1000)}'
# username = 'emqx'
# password = 'public'

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client):
    # msg_count = 1
    # while True:
    #     time.sleep(1)
    #     msg = f"messages: {msg_count}"
    #     result = client.publish(topic, msg)
    #     # result: [0, 1]
    #     status = result[0]
    #     if status == 0:
    #         print(f"Send `{msg}` to topic `{topic}`")
    #     else:
    #         print(f"Failed to send message to topic {topic}")
    #     msg_count += 1
    #     if msg_count > 5:
    #         break
    pub = Publisher(client=client, name="name",id=0)
    pub.qos=0
    pub.publish_loop("test/topic")


def run():
    client = connect_mqtt()
    #client2 = connect_mqtt()
    
    # pub2 = Publisher(client=client2, qos=0, delay=0.2, name="yo", id=2)
    pub = Publisher(1,'localhost', 1883)
    
    pub.start()
    
    
    # pub.publish_loop(topic='counter/1')
    # pub2.publish_loop(topic='counter/2')
    



if __name__ == '__main__':
    run()
