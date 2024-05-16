import time
import sys
from paho.mqtt import client as mqtt_client

timeout_value = 2

class Publisher:
    def __init__(self, instance_id, broker, port):
        self.client = mqtt_client.Client(f"Publisher-instance-{instance_id}")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.broker = broker
        self.port = port 
        self.instance_id = instance_id
        self.qos = 0
        self.delay = 0
        self.instancecount = 1
        self.receieved = [False, False, False]
        self.should_publish = False
        self.should_reconnect = True
    
    def on_connect(self, client, userdata, flags, rc):
        print(f"Publisher'{self.instance_id} 'successfully connected to broker")
        client.subscribe('request/qos')
        client.subscribe('request/delay')
        client.subscribe('request/instancecount')

    def on_message(self, client, userdata, msg):
        message = msg.payload.decode()
        topic = msg.topic
        if topic == 'request/qos':
            self.qos = int(message)
            self.receieved[1] = True
        elif topic == 'request/delay':
            self.delay = int(message)
            self.receieved[2] = True
        elif topic == 'request/instancecount':
            self.instancecount = int(message)
            self.receieved[0] = True
        if self.instancecount >= self.instance_id and False not in self.receieved:
            self.should_publish = True
            
    


    def on_disconnect(self, client, userdata, rc):
        #logging.info("Disconnected with result code: %s", rc)
        if not self.should_reconnect:
            return
        FIRST_RECONNECT_DELAY = 1
        RECONNECT_RATE = 2
        MAX_RECONNECT_COUNT = 12
        MAX_RECONNECT_DELAY = 60
        reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY
        while reconnect_count < MAX_RECONNECT_COUNT:
            #logging.info("Reconnecting in %d seconds...", reconnect_delay)
            time.sleep(reconnect_delay)

            try:
                client.reconnect()
                #logging.info("Reconnected successfully!")
                print(f"Publishser {self.instance_id} Reconnected to broker successfully")
                return
            except Exception as err:
                print("%s. Reconnect failed for %d. Retrying...", err, self.instance_id)

            reconnect_delay *= RECONNECT_RATE
            reconnect_delay = min(reconnect_delay, MAX_RECONNECT_DELAY)
            reconnect_count += 1
        print("Reconnect failed after %s attempts for instance %d. Exiting...", reconnect_count, self.instance_id)


    def publish_loop(self):
        topic = f"counter/{self.instance_id}/{self.qos}/{self.delay}"
        counter = 0
        start_time = time.time()
        while time.time() - start_time < timeout_value:
            self.client.publish(topic=topic, payload=str(counter), qos=self.qos )
            counter += 1
            time.sleep(self.delay/1000)

    def start(self):
        self.client.connect(self.broker, self.port)
        #self.client.loop_forever()
        self.client.loop_start()
        try:
            while True:
                if self.should_publish:
                    print(f'instance {self.instance_id} publishing to {self.instance_id}/{self.qos}/{self.delay}')
                    self.should_publish = False
                    self.receieved = [False, False, False]
                    self.publish_loop()
                time.sleep(1)
        except:
            self.client.loop_stop()
            self.should_reconnect = False
            self.client.disconnect()
            


def main(id, broker='localhost', port=1883):
    publisher = Publisher(id, broker, port)
    publisher.start()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: id [broker] [port]")
        sys.exit(1)
    pubid = int(sys.argv[1])
    broker = sys.argv[2] if len(sys.argv) > 2 else None
    port = int(sys.argv[3]) if len(sys.argv) > 3 else None
    if broker and port:
        main(pubid, broker, port)
    elif broker:
        main(pubid, broker)
    else:
        main(pubid)

