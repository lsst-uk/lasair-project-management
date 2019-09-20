from subprocess import run, PIPE
import time
import common.settings as settings

print('------------- SHERLOCK -------------')
t = time.time()
timeout = 1800
cmd = settings.LASAIR_ROOT + 'anaconda3/envs/sherlock/bin/sherlock -N dbmatch --update | grep -v password'
process = run(cmd, stdout=PIPE, stderr=PIPE, shell=True, timeout=timeout)
print(process.stdout)
print(process.stderr)
print('Sherlock finished in %.1f seconds' % (time.time() - t))
