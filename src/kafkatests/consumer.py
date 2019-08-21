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

class Consumer():
    def __init__(self, group_id):
        self.group_id = group_id

    def run(self):

# this topic has preceisely 1000 candidates from May 2018
#        topic = 'ztf_test'
        topic = 'ztf_20190821_programid1'


        schema_files = [
            "ztf-avro-alert/schema/candidate.avsc",
            "ztf-avro-alert/schema/cutout.avsc",
            "ztf-avro-alert/schema/prv_candidate.avsc",
            "ztf-avro-alert/schema/alert.avsc"]

        conf = {
            'bootstrap.servers': 'public.alerts.ztf.uw.edu:9092',
#            'bootstrap.servers': 'Stedigo:9092',
            'group.id': self.group_id,
            'default.topic.config': {'auto.offset.reset': 'smallest'}
        }
        print(conf)
        print("Topic: %s, " %topic)

        frombeginning = False
        streamReader = alertConsumer.AlertConsumer(topic, frombeginning, schema_files, **conf)
        streamReader.__enter__()

        ialert = 0
        while 1:
            t = time.time()
            try:
                msg = streamReader.poll(decode=True, timeout=60)
            except alertConsumer.EopError as e:
                print('got EopError')
                print(e)
                break

            if msg is None:
                print('null message received')
                break
            for alert in msg:
                data = msg_text(alert)
                ialert += 1
                print(ialert)

        # looks like thats all the alerts we will get
        streamReader.__exit__(0,0,0)
        print("Exiting %s with %d events" % (self.name, ialert))

################
start = time.time()
group_id = 'LASAIR-test%03d' % random.randrange(1000)
c = Consumer(group_id)
c.run()
print('======= %.1f seconds =========' % ((time.time()-start)))
