#!/usr/bin/python

import simplejson
import httplib
import urllib
import time
import sys

class Client:
    def __init__(self, queue, host='127.0.0.1', port=9094):
        hoststr = '%s:%d' % (host, port)

        self.conn = httplib.HTTPConnection(hoststr)
        self.queue = queue

    def enqueue(self, data):
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "text/plain"
        }

        params = 'data=' + urllib.quote(simplejson.dumps(data))
        self.conn.request("POST", '/' + self.queue, params, headers)
        res = self.conn.getresponse()

    def dequeue(self, func):
        while 1:
            res = self.conn.request("GET", self.queue)

            if res.status == 200:
                func(simplejson.loads(res.read())) 
            elif res.status == 404:
                time.sleep(1)
            else:
                print res.status
                sys.exit(1)      
