#!/usr/bin/python

import simplejson
import httplib
import urllib
import time
import sys

class Client:
    def __init__(self, queue, host='127.0.0.1', port=9094):
        self.host = host
	self.port = port
        self.queue = queue

    def _getconnection(self):
	hoststr = '%s:%d' % (self.host, self.port)
	return httplib.HTTPConnection(hoststr)
	

    def enqueue(self, data):
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "text/plain"
        }

	conn = self._getconnection()

        conn.request("POST", '/' + self.queue, simplejson.dumps(data), headers)
        res = conn.getresponse()
	conn.close()

    def dequeue(self, func):
        while 1:
	    conn = self._getconnection()

            conn.request("GET", '/' + self.queue)
	    res = conn.getresponse()
	    #conn.close()

            if res.status == 200:
                func(simplejson.loads(res.read())) 
            elif res.status == 404:
                time.sleep(1)
            else:
                print res.status
                sys.exit(1)      
