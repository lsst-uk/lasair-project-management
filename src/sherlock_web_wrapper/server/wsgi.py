from cgi import parse_qs, escape
#from subprocess import Popen, PIPE

def application(environ, start_response):
# the environment variable CONTENT_LENGTH may be empty or missing
    try:
        request_body_size = int(environ.get('CONTENT_LENGTH', 0))
    except (ValueError):
        request_body_size = 0

    # When the method is POST the variable will be sent
    # in the HTTP request body which is passed by the WSGI server
    # in the file like wsgi.input environment variable.
    request_body = environ['wsgi.input'].read(request_body_size)
    d = parse_qs(request_body)

    json_request = d.get('json_request', [''])[0] # Returns the first age value.
    s = b'[ {"name":"ATLAS17maj","ra":75.80455, "dec":-22.83298}]'

    from sherlock_driver import run_sherlock


#    s = s.encode('UTF-8')

#    cmdlist = ['/data/anaconda/envs/sherlock/bin/python',
#        '/home/lasair/lasair/src/sherlock_web_wrapper/server/sherlock_driver.py',
#        ' -s', '/home/lasair/lasair/src/sherlock_web_wrapper/server/sherlock_script_settings.yaml']

#    cmdlist = ['/data/anaconda/envs/sherlock/bin/python', '/home/lasair/lasair/src/sherlock_web_wrapper/server/echo.py']
#    s = 'fifi'

#    process = Popen(cmdlist, stdout=PIPE, stdin=PIPE, stderr=PIPE)
#    process = Popen(cmdlist, stdout=PIPE, stderr=PIPE, shell=True)
#    stdout, stderr = process.communicate(input = s)
#    stdout, stderr = process.communicate()

#    stdout = stdout.decode('utf-8')
#    stderr = stderr.decode('utf-8')

    response = s
    status = '200 OK'
    headers = [('Content-type', 'text/plain')]
    start_response(status, headers)
    return [response]
