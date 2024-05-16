import time
import sys
import csv
import numpy as np
import pandas as pd
from paho.mqtt import client as mqtt_client
from publisher import timeout_value

class Analyser:
    def __init__(self, broker, port):
        self.client = mqtt_client.Client(f"Analyser")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.broker = broker 
        self.port = port
        self.start_time = 0
        self.qos = 0
        self.anqos = 0
        self.instancecount = 1
        self.delay = 0
        self.statmap = {}
        self.brokerstats = {}
        # intitialise the map to store all information 
        self.set_statmap()
        self.set_brokerstats()
    
    def set_brokerstats(self):
        for i in range(1, 6):
            for j in range(3):
                for k in range(4):
                    delay = 4 if k == 3 else k
                    self.brokerstats[f"{i}/{j}/{delay}"] = {
                        'msgs_recv_broker' : 0,
                        'sockets' : 0,
                        'published_dropped' : 0,
                        'msgs_sent_broker' : 0
                    }

    def set_statmap(self):
        for i in range(1,6): #instancecount
            for j in range(3): # qos
                for k in range(4): # delay
                    delay = 4 if k == 3 else k
                    # self.statmap[f"counter/{i}/{j}/{delay}"] = [{
                    #     'msgcount' : 0,
                    #     'current_val' : 0,
                    #     'max_val' : 0,
                    #     'out_order_count': 0,
                    #     'last_msg_time' : 0,
                    #     'delays' : []
                    # } for l in range(6, i, -1)]
                    self.statmap[f"{i}/{j}/{delay}"] = [{
                        'msgcount' : -1, # counter on pub side starts at 0, will overcount by 1
                        'current_val' : 0,
                        'max_val' : 0,
                        'out_order_count': 0,
                        'last_msg_time' : 0,
                        'delay_sum' : 0,
                        'in_order_count' : 0,
                        'delays' : [0 for m in range(10)]
                    } for l in range(1, i+1)]
                    """
                    need a new map for each instance. 
                    (instance 1 will publish 5*j*k times, 2 4*j*k times etc)
                    """

    def write_csv(self, qos):
        df = self.compute_stats()
        file_path = f'output-{qos}.csv'
        df.to_csv(file_path, index=False)
        
    def write_raw_data(self, qos):
        data = [['msgcount', 'current', 'max', 'instance', 'topic']]
        for maparr in self.statmap.items():
            for (i,stats) in enumerate(maparr[1]):
                msg = str(stats['msgcount'])
                cur = str(stats['current_val'])
                max = str(stats['max_val'])
                topic = maparr[0]
                instance = str(i)
                data.append([msg, cur, max, instance, topic])
        file_path = f'raw-{qos}.csv'
        # Writing data to CSV file
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(data)

    def compute_stats(self):
        # this will be a dataframe 
        stats = {
            "instancecount" : [],
            "qos" : [],
            "delay": [],
            "msg_rate" : [],
            "loss_rate": [],
            "out_of_order": [],
            "average_delay": [],
            'msgs_recv_broker' : [],
            'msgs_sent_broker' : [],
            'sockets' : [],
            'published_dropped' : [],
            'msgs_sent_broker' : []
        }
        for maparr in self.statmap.items():
            # maparr[0] is the 'instancecount/qos/delay' maparr[1] are the values 
            total_msgs = sum(map(lambda val: val['msgcount'], maparr[1]))
            msg_rate = round(total_msgs/timeout_value, 4)
            current_vals = sum(map(lambda val: val['current_val'], maparr[1]))
            loss_rate = round(1 - (total_msgs/current_vals), 4) if current_vals > 0 else 0 # avoid divide by 0
            out_of_order = sum(map(lambda val: val['out_order_count'], maparr[1]))
            out_order_rate = round(out_of_order/total_msgs, 4) if total_msgs > 0 else 0
            total_delay = sum(map(lambda val: val['delay_sum'], maparr[1]))
            total_inorder = sum(map(lambda val: val['in_order_count'], maparr[1]))
            average_delay = round(total_delay/total_inorder, 4) if total_inorder > 0 else 0
            topics = maparr[0].split("/")
            # broker stats 
            brokerstats = self.brokerstats[maparr[0]] 
            msg_recv_broker = brokerstats['msgs_recv_broker']
            sockets = brokerstats['sockets']
            published_dropped = brokerstats['published_dropped']
            msgs_sent_broker = brokerstats['msgs_sent_broker']

            stats["instancecount"].append(topics[0])
            stats["qos"].append(topics[1])
            stats["delay"].append(topics[2])
            stats["msg_rate"].append(msg_rate)
            stats["loss_rate"].append(loss_rate)
            stats["out_of_order"].append(out_order_rate)
            stats["average_delay"].append(average_delay)
            stats['msgs_recv_broker'].append(msg_recv_broker)
            stats["sockets"].append(sockets)
            stats['published_dropped'].append(published_dropped)
            stats['msgs_sent_broker'].append(msgs_sent_broker)
        return pd.DataFrame(stats)

                
    
    def on_connect(self, client, userdata, flags, rc):
        print("Analyser successfully connected to broker")
        # set up all subs 
        # for i in range(5): #instancecount
        #     for j in range(3): # qos
        #         for k in range(4): # delay
        #             delay = 4 if k == 3 else k
        #             client.subscribe(f"counter/{i}/{j}/{delay}")
        client.subscribe('counter/#', qos=self.anqos)
        client.subscribe('$SYS/broker/load/messages/received/1min')
        client.subscribe('$SYS/broker/load/messages/sent/1min')
        client.subscribe('$SYS/broker/load/sockets/1min')
        client.subscribe('$SYS/broker/load/publish/dropped/1min')
        

        
        
    
    def handle_counter(self, topic, message):
        
        subtopics = topic.split("/")
        instance = int(subtopics[1])
        qos = subtopics[2]
        delay = int(subtopics[3])
        current_time = time.time()
        if self.start_time == 0 and delay == self.delay:
            self.start_time = time.time()
        # stop recording after 60 seconds, ensure that any 'trickle' messages are not recorded 
        if current_time - self.start_time >= timeout_value or delay != self.delay:
            return
        
        
        # delay is the inner loop, so this value will change every 
        # cycle. If we receive a different delay this is from previous publish loop
        if delay != self.delay:
            return
        current_val = int(message)
        # len(instance1 arraymap) == 5 le(instance5 arraymap) == 1
        statmap = self.statmap[f'{self.instancecount}/{qos}/{delay}'][instance - 1]
        # handle the inter-delay first 
        last_msg_time = statmap['last_msg_time']
        
        if last_msg_time != 0 and current_val == 1 + statmap['current_val']: # ensure they are consecutive
            statmap['delay_sum'] += (current_time - last_msg_time) * 1000 #to record in terms of ms
            statmap['in_order_count'] += 1
        statmap['last_msg_time'] = current_time

        statmap['msgcount'] += 1
        statmap['current_val'] = current_val
        if statmap['max_val'] > current_val:
            statmap['out_order_count'] += 1
        else:
             statmap['max_val'] = current_val
        # FIXME
        # cur_time = time.time()
        # statmap['delays'].append(cur_time)

    def handle_sys(self, topic, message):
        brokerstats = self.brokerstats[f'{self.instancecount}/{self.qos}/{self.delay}']
        if topic == '$SYS/broker/load/messages/received/1min':
            brokerstats['msgs_recv_broker'] = message
        elif topic == '$SYS/broker/load/sockets/1min':
            brokerstats['sockets'] = message
        elif topic == '$SYS/broker/load/publish/dropped/1min':
            brokerstats['published_dropped'] = message
        elif topic == '$SYS/broker/load/messages/sent/1min':
            brokerstats['msgs_sent_broker'] = message
 
            

    def on_message(self, client, userdata, msg):
        message = msg.payload.decode()
        topic = msg.topic
        subtopics = topic.split("/")
        if subtopics[0] == 'counter':
            self.handle_counter(topic, message)
        if subtopics[0] == '$SYS':
            self.handle_sys(topic, message)

    def start(self):
        self.client.connect(self.broker, self.port)
        self.client.loop_start()
        try:
            #self.client.loop_forever()
            tests_remain = 180
            for anqos in range(3):
                self.anqos = anqos
                self.client.unsubscribe('counter/#')
                self.client.subscribe('counter/#', qos=anqos)
                for qos in range(3): # qos 
                    self.qos = qos
                    self.client.publish('request/qos', qos)
                    time.sleep(1.5) # ensure it is received before the delay, which will trigger sending
                    for i in range(1,6):
                        self.instancecount = i 
                        self.client.publish('request/instancecount', i)
                        time.sleep(1.5)
                        for d in range(4):
                            delay = 4 if d == 3 else d
                            self.delay = delay
                            self.client.publish('request/delay', delay)
                            self.start_time = 0
                            print(f"publishing with sub qos={anqos}, instancecount={i}, qos={qos}, delay={delay}. Tests remaining: {tests_remain}")
                            tests_remain -= 1
                            while self.start_time == 0: # will get set in message callback
                                time.sleep(1)
                            print('received')
                            for p in range(timeout_value):
                                time.sleep(1)
                            #time.sleep(120) 
                             # allow extra time for the backlog
                print(f"writing data for analyser qos={anqos}")
                self.write_csv(anqos)
                self.write_raw_data(anqos)
                self.set_statmap() # reset the statmap to zeros

        except:
            #self.client.loop_stop()
            #print(self.statmap['1/0/0'])
            self.write_csv(-1)
            self.write_raw_data(-1)
        self.client.loop_stop()
        
    
an = Analyser('192.168.8.250', 1883)
an.start()


