from subprocess import Popen, PIPE

def main():
    cmdlist = ['/data/anaconda/envs/sherlock/bin/python', 'echo.py']
#    cmdlist = ['/data/anaconda/envs/sherlock/bin/python',
#        '/home/lasair/lasair/src/sherlock_web_wrapper/server/sherlock_driver.py',
#        ' -s', '/home/lasair/lasair/src/sherlock_web_wrapper/server/sherlock_script_settings.yaml']
    s = open('example.json').read()

    process = Popen(cmdlist, stdout=PIPE, stdin=PIPE, stderr=PIPE)
#    process = Popen(cmdlist, stdout=PIPE, stderr=PIPE)
#    stdout, stderr = process.communicate(input = s)
    stdout, stderr = process.communicate(input = s)

#    stdout = stdout.decode('utf-8')
#    stderr = stderr.decode('utf-8')

    print(stdout)

main()
