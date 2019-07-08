import os
import time

print('------------- SHERLOCK -------------')
t = time.time()
cmd = '/home/roy/anaconda3/envs/sherlock/bin/sherlock -N dbmatch --update | grep -v password'
os.system(cmd)
print('Sherlock finished in %.1f seconds' % (time.time() - t))
