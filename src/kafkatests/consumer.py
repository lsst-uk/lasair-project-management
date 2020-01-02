from confluent_kafka import Consumer, KafkaError
import time
import random
import time

class msgConsumer():
    def __init__(self, kafka_server, group_id):
        self.group_id = group_id

        conf = {
            'bootstrap.servers': kafka_server,
            'group.id': self.group_id,
            'default.topic.config': {'auto.offset.reset': 'smallest'}
        }
        self.streamReader = Consumer(conf)

    def topics(self):
        t = self.streamReader.list_topics()
        t = t.topics
        t = t.keys()
        t = list(t)
        return t

    def subscribe(self, topic):
        self.streamReader.subscribe([topic])

    def poll(self):
        try:
            msg = self.streamReader.poll(timeout=20)
            return msg.value()
        except:
            return None

    def close(self):
        self.streamReader.close()

################
import sys
if len(sys.argv) < 2:
    print('Usage: Consumer.py server:port <topic> ')
    sys.exit()

kafka_server = sys.argv[1]
group_id = 'LASAIR2'
c = msgConsumer(kafka_server, group_id)

if len(sys.argv) < 3:
    print('Topics are ', c.topics())
    sys.exit()
else:
    topic = sys.argv[2]
    c.subscribe(topic)
    start = time.time()
    count = 0
    while 1:
        msg = c.poll()
        count += 1
        if count % 10000 == 0: print(count)
        #print(msg)
        if not msg:
            break
    print('%d messages in %.1f seconds =========' % (count, (time.time()-start)))
