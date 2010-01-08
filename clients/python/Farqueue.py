#!/usr/bin/python

import simplejson
import httplib
import urllib
import time
import sys
import os 
import md5
import signal

class Continue(Exception):
    def __init__(self, data):
        self.value = data
    def __str__(self):
       return repr(self.value) 

class Client:
    def __init__(self, queue, host='127.0.0.1', port=9094):
        self.host = host
	self.port = port
        self.queue = queue

    def _getconnection(self):
	hoststr = '%s:%d' % (self.host, self.port)
	return httplib.HTTPConnection(hoststr)
	
    def handle_timeout(self, signum, frame): 
        raise TimeoutFunctionException()

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

    def message(self, data={}, timeout=10):
        id = md5.new(str(os.getpid) + str(int(time.time()))).hexdigest()

        old = signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(timeout)

        result = None

        message = {
            'id': id,
            'data': data,
        }

        queue = self.queue

        self.enqueue(message)

        self.queue = queue + id

        def func(data):
            result = data
            raise Continue(data)

        try:
            self.dequeue(func)
        except Continue as c:
            result = c.value
            pass 
        finally:
            self.queue = queue
            signal.signal(signal.SIGALRM, old)

        signal.alarm(0)

        return result

    def subscribe(self, func):
        def process(message):
            data = message['data']
            id = message['id']

            res = func(data)

            queue = self.queue
            self.queue = queue + id

            try:
                self.enqueue(res)
            finally:
                self.queue = queue

        self.dequeue(process)
