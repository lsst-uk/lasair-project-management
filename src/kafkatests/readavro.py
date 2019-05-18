import os
import fastavro
dir = '/data/ztf/data'

for fname in os.listdir(dir):
    if not fname.endswith('.avro'):
        continue

    print(fname)
    f = open(dir + '/' + fname)
    freader = fastavro.reader(f)
    for packet in freader:
        print(packet['candid'], packet['jd'])
