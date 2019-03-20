'''
Created on Mar 04, 2019

@author: stv@roe.ac.uk
'''

import requests
from argparse import ArgumentParser
import json 

class SlackSender(object):
    '''
    Send messages to a slack channel
    '''


    def __init__(self, webhook_url):
        '''
        Constructor
        '''
        self.webhook = webhook_url
        return
     


    def send (self, text):
        '''
        Send a message to a Slack Channel, given a webhook url
        '''
        headers = {'content-type': 'application/json'}
        r = requests.post(self.webhook, headers=headers, json=json.dumps({'text': text}), data=json.dumps({'text': text}))


        print (r.text)
        print(r.status_code, r.reason)
        return r.text


def main():

    '''
    Validator Arguments
    '''
    parser = ArgumentParser()
    parser.add_argument("-url", "--webhook_url", dest="webhook_url",
                    help="WebHook URL", metavar="Webhook")
   

    args = parser.parse_args()    


    slack_sender = SlackSender(args.webhook_url)
    slack_sender.send("Testing")

if __name__ == "__main__":
    main()
