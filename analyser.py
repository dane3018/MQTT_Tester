import time
import sys
import csv
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
        """
        Statmap will store all publisher stats throughout the program. This map 
        has keys as {instancecount}/{qos}/{delay} and values are an array of length 
        instancecount that will record the stats relating to each publisher currently 
        publishing
        """
        self.statmap = {}
        # stores broker stats 
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
        """
        Computes the stats using the instance's statmap and writes this data to a 
        csv file
        """
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
        """
        Will take the self.statmap and compute the stats required by the assignment 
        specs. Returns a dataframe, to be easily converted to csv.
        """
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
            max_vals = sum(map(lambda val: val['max_val'], maparr[1]))
            loss_rate = round(1 - (total_msgs/max_vals), 4) if max_vals > 0 else 0 # avoid divide by 0
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
        print(f"Analyser successfully connected to broker at {self.broker} and port {self.port}")
        client.subscribe('counter/+/+/+', qos=self.anqos)
        client.subscribe('$SYS/broker/load/messages/received/1min')
        client.subscribe('$SYS/broker/load/messages/sent/1min')
        client.subscribe('$SYS/broker/load/sockets/1min')
        client.subscribe('$SYS/broker/load/publish/dropped/1min')
        

        
        
    
    def handle_counter(self, topic, message):
        """
        Messages with counter/# topic will be filtered here. Updates stats relating 
        to the current qos, delay and instance count.
        """
        subtopics = topic.split("/")
        instance = int(subtopics[1])
        qos = int(subtopics[2])
        delay = int(subtopics[3])
        current_time = time.time()
        if self.start_time == 0 and delay == self.delay:
            self.start_time = time.time()
        # stop recording after 60 seconds, ensure that any 'trickle' messages are not recorded 
        if current_time - self.start_time >= timeout_value or delay != self.delay or qos != self.qos :
            return
        
        current_val = int(message)
    
        # the currently running publishers stats will get updated here
        statmap = self.statmap[f'{self.instancecount}/{qos}/{delay}'][instance - 1]
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
        """
        on message callback for all subscribed topics 
        """
        message = msg.payload.decode()
        topic = msg.topic
        subtopics = topic.split("/")
        if subtopics[0] == 'counter':
            self.handle_counter(topic, message)
        if subtopics[0] == '$SYS':
            self.handle_sys(topic, message)
    
    def wait_for_response(self):
        """
        Will wait for 15 seconds for a response then resend the request to the publisher 
        after 3 attempts will return False, if a response is received returns true.
        """
        has_recv = False
        republish_attempts = 3
        while not has_recv:
            # wait for 15 seconds before re-sending
            for q in range(15):
                if self.start_time != 0:
                    republish_attempts = 3
                    has_recv = True
                    break
                time.sleep(1)
            republish_attempts -= 1
            if has_recv:
                break
            if republish_attempts < 0:
                return False
            self.client.publish('request/qos', self.qos)
            time.sleep(1.5)
            self.client.publish('request/instancecount', self.instancecount)
            time.sleep(1.5)
            self.client.publish('request/delay', self.delay)
            print(f"re-publishing with sub qos={self.anqos}, instancecount={self.instancecount}, qos={self.qos}, delay={self.delay}.")
        return True
    
    def run_once(self):
        """
        Used to run the bonus question. Requires other configuration to other code to work 
        """
        self.client.connect(self.broker, self.port)
        self.client.loop_start()
        self.client.publish('request/qos', 0)
        self.client.publish('request/delay', 0)
        self.client.publish('request/instancecount', 10)
        self.qos = 0
        self.delay = 0
        self.instancecount = 10
        result = self.wait_for_response()
        time.sleep(60)
        self.write_csv(-1)


    def start(self):
        """
        Runs the analyser program loop. Will write a csv for each iteration 
        of anqos. 
        """
        self.client.connect(self.broker, self.port)
        self.client.loop_start()
        try:
            tests_remain = 180
            for anqos in range(3): # analyser qos level
                self.anqos = anqos
                self.client.unsubscribe('counter/+/+/+')
                self.client.subscribe('counter/+/+/+', qos=anqos)
                for qos in range(3): # qos 
                    for i in range(1,6): # instancecount
                        for d in range(4): # delay
                            # publish to all topics at every iteration, so there is never a publisher sending 
                            # to the wrong topic 
                            self.qos = qos
                            self.client.publish('request/qos', qos)
                            self.instancecount = i
                            self.client.publish('request/instancecount', i)
                            delay = 4 if d == 3 else d
                            self.delay = delay
                            self.client.publish('request/delay', delay)
                            self.start_time = 0
                            print(f"publishing with sub qos={anqos}, instancecount={i}, qos={qos}, delay={delay}. Tests remaining: {tests_remain}")
                            tests_remain -= 1
                            # handle case when a response is not received
                            result = self.wait_for_response()
                            if not result:
                                print("unable to receive response, skipping")
                                continue
                            print('received')
                            for p in range(timeout_value):
                                time.sleep(1)
                            #time.sleep(120) 
                             # allow extra time for the backlog
                print(f"writing data for analyser qos={anqos}")
                self.write_csv(anqos)
                self.write_raw_data(anqos)
                self.set_statmap() # reset the statmap to zeros

        except KeyboardInterrupt as e:
            #self.client.loop_stop()
            #print(self.statmap['1/0/0'])
            self.write_csv(-1)
            self.write_raw_data(-1)
            print(e)
        self.client.loop_stop()
        
def main(broker='localhost', port=1883):
        an = Analyser(broker, port)
        an.start()

if __name__ == '__main__':
    broker = sys.argv[1] if len(sys.argv) > 1 else None
    port = int(sys.argv[2]) if len(sys.argv) > 2 else None
    if broker and port:
        main(broker, port)
    elif broker:
        main(broker)
    else:
        main()

