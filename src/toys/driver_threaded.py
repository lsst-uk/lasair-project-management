import threading
import toys
import time

# This is the maximum number of objects 
n_objects = 1000000

class addem_thread(threading.Thread):
    def __init__(self, threadID, nthread, number):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.number = number

    def run(self):
        t = time.time()
        for i in range(self.number):
            toys.make_candidate(n_objects, debug=False)
        print("---- %d candidates made in %.2f seconds" % (self.number, time.time()-t))

###################
import sys
toys.start_again()
print("---- Tables remade")

if len(sys.argv) < 3:
    print('Usage: Consumer.py nthread number')
    sys.exit()
else:
    nthread = int(sys.argv[1])
    number  = int(sys.argv[2])

start = time.time()
thread_list = []
for t in range(nthread):
    thread_list.append(addem_thread(t, nthread, number))

# start them up
for th in thread_list:
     th.start()

# wait for them to finish
for th in thread_list:
     th.join()
print('======= %.1f seconds =========' % ((time.time()-start)))
