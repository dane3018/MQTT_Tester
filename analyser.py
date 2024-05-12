import time
import sys
import csv
import numpy as np
from paho.mqtt import client as mqtt_client



class Analyser:
    def __init__(self, broker, port):
        self.client = mqtt_client.Client(f"Analyser")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.broker = broker 
        self.port = port
        self.start_time = 0
        self.anqos = 0
        self.pubqos = 0
        self.instancecount = 1
        self.delay = 0
        self.statmap = {}
        # intitialise the map to store all information 
        for i in range(1,6): #instancecount
            for j in range(3): # qos
                for k in range(4): # delay
                    delay = 4 if k == 3 else k
                    self.statmap[f"counter/{i}/{j}/{delay}"] = [{
                        'msgcount' : 0,
                        'current_val' : 0,
                        'max_val' : 0,
                        'out_order_count': 0,
                        'last_msg_time' : 0,
                        'delays' : []
                    } for l in range(6, i, -1)]
                    """
                    need a new map for each instance. 
                    (instance 1 will publish 5*j*k times, 2 4*j*k times etc)
                    """
    def write_csv(self):
        data = [['msgcount', 'current', 'max', 'topic']]
        for maparr in self.statmap.items():
            print(maparr)
            for stats in maparr[1]:
                msg = str(stats['msgcount'])
                cur = str(stats['current_val'])
                max = str(stats['max_val'])
                topic = maparr[0]
                data.append([msg, cur, max, topic])
        file_path = 'output.csv'
        # Writing data to CSV file
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(data)

    def convert_stats(self):
        for maparr in self.statmap.items():
            instance = maparr[0].split("/")[1]
            for (i, stat) in enumerate(maparr[1]):
                pass





    
    def on_connect(self, client, userdata, flags, rc):
        print("Analyser successfully connected to broker")
        # set up all subs 
        # for i in range(5): #instancecount
        #     for j in range(3): # qos
        #         for k in range(4): # delay
        #             delay = 4 if k == 3 else k
        #             client.subscribe(f"counter/{i}/{j}/{delay}")
        client.subscribe('counter/#', qos=0)
        
    
    def handle_counter(self, topic, message):
        if self.start_time == 0:
            self.start_time = time.time()
        current_time = time.time()
        if current_time - self.start_time >= 5:
            return
        subtopics = topic.split("/")
        instance = int(subtopics[1])
        qos = subtopics[2]
        delay = subtopics[3]
        current_val = int(message)
        # len(instance1 arraymap) == 5 le(instance5 arraymap) == 1
        index = self.instancecount - (instance - 1) - 1
        statmap = self.statmap[topic][index]
        statmap['msgcount'] += 1
        statmap['current_val'] = current_val
        if statmap['max_val'] > current_val:
            statmap['out_order_count'] += 1
        else:
             statmap['max_val'] = current_val
        # FIXME
        # cur_time = time.time()
        # statmap['delays'].append(cur_time)

 

    def on_message(self, client, userdata, msg):
        message = msg.payload.decode()
        topic = msg.topic
        subtopics = topic.split("/")
        if subtopics[0] == 'counter':
            self.handle_counter(topic, message)

    def start(self):
        self.client.connect(self.broker, self.port)
        self.client.loop_start()
        try:
            #self.client.loop_forever()
            for k in range(3):
                self.client.publish('request/qos', k)
                time.sleep(2) # ensure it is received
                for j in range(4):
                    delay = 4 if j == 3 else j
                    self.client.publish('request/delay', delay)
                    time.sleep(2)
                    for i in range(1, 6):
                        self.start_time = 0
                        self.instancecount = i
                        self.client.publish('request/instancecount', i)
                        print(f"publishing with delay={delay}, qos={k}, instancecount={i}")
                        while self.start_time == 0:
                            time.sleep(1)
                        time.sleep(10)

        except:
            #self.client.loop_stop()
            print(self.statmap['counter/1/0/0'])
            self.write_csv()
        for i in range(1,6):
            print(self.statmap[f'counter/{i}/0/0'], "\n")
        
    
an = Analyser('localhost', 1883)
an.write_csv()


