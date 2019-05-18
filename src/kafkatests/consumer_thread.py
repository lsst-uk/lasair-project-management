# Multi threaded consuming of ZTF alerts

# conda config --add channels conda-forge
# conda install python-confluent-kafka
# conda install -c conda-forge python-avro
# conda install -c conda-forge fastavro

from confluent_kafka import Consumer, KafkaError
import threading, logging, time
import alertConsumer
import random
import time

def msg_text(message):
    """Remove postage stamp cutouts from an alert message.
    """
    message_text = {k: message[k] for k in message
                    if k not in ['cutoutDifference', 'cutoutTemplate', 'cutoutScience']}
    return message_text

class Consumer(threading.Thread):
    def __init__(self, threadID, nthread, group_id):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.group_id = group_id

    def run(self):
        print("Starting " + self.name)

# this topic has preceisely 1000 candidates from May 2018
        topic = 'ztf_test'

        schema_files = [
            "ztf-avro-alert/schema/candidate.avsc",
            "ztf-avro-alert/schema/cutout.avsc",
            "ztf-avro-alert/schema/prv_candidate.avsc",
            "ztf-avro-alert/schema/alert.avsc"]

        conf = {
            'bootstrap.servers': 'public.alerts.ztf.uw.edu:9092',
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'group.id': self.group_id
        }

        frombeginning = False
        streamReader = alertConsumer.AlertConsumer(topic, frombeginning, schema_files, **conf)
        streamReader.__enter__()

        totalert = 0
        while 1:
            ialert = 0
            # get all the events we can with a timeout of 60 sec
            while 1:
                try:
                    msg = streamReader.poll(decode=True, timeout=60)
                    if msg is None:
                        continue
                    for alert in msg:
                        data = msg_text(alert)
                        ialert += 1
                except alertConsumer.EopError as e:
                    print(self.name, e.message)
                    break
            print(self.name, 'got %d, sleeping 10 ...' % ialert)
            totalert += ialert
            # if you got some alerts, lets sleep a bit and try again
            if ialert == 0: break
            time.sleep(10)

        # looks like thats all the alerts we will get
        streamReader.__exit__(0,0,0)
        print("Exiting %s with %d events" % (self.name, totalert))

################
import sys
if len(sys.argv) < 2:
    print('Usage: Consumer.py nthread')
    sys.exit()
else:
    nthread = int(sys.argv[1])

# make the thread list
group_id = 'LASAIR-test%03d' % random.randrange(1000)
print('using group_id %s' % group_id)

start = time.time()
thread_list = []
for t in range(nthread):
    thread_list.append(Consumer(t, nthread, group_id))
    
# start them up
for th in thread_list:
     th.start()
    
# wait for them to finish
for th in thread_list:
     th.join()
print('======= %.1f seconds =========' % ((time.time()-start)))
