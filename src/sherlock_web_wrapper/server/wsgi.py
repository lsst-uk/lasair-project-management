def application(environ, start_response):
    body = 'Hello Roy!\n'
    status = '200 OK'
    headers = [('Content-type', 'text/plain')]
    start_response(status, headers)
    return [body]
