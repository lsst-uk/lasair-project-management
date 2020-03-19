import requests
json_request = open('example.json').read()
url = 'http://lasair-dev-node0/'
response = requests.post(url, data={'json_request': json_request})
print(response)
print(response.content)
