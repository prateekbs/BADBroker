#!/usr/bin/env python3

import tornado.httpclient
import tornado.gen
import simplejson as json
import time
import threading
import pika

def async_call(URL, service_point, post_data, callback, **kwargs):    
    http_client = tornado.httpclient.AsyncHTTPClient()
    
    try:
        request = tornado.httpclient.HTTPRequest(URL + "/" + service_point, method='POST',
                body=json.dumps(post_data), request_timeout = 5)
        http_client.fetch(request, callback, **kwargs)        
    
    except tornado.httpclient.HTTPError as e:
        print('Error:', str(e))
    except Exception as e:
        print('Error:', str(e))
    
           
def service_call(URL, service_point, post_data):
    http_client = tornado.httpclient.HTTPClient()
    try:        
        request = tornado.httpclient.HTTPRequest(URL + "/" + service_point, method = 'POST', body = json.dumps(post_data))            
        response = http_client.fetch(request)
        return json.loads(response.body)
        
    except tornado.httpclient.HTTPError as e:
        # HTTPError is raised for non-200 responses; the response
        # can be found in e.response.
        print("Error: " + str(e))
    except Exception as e:
        # Other errors are possible, such as IOError.
        print("Error: " + str(e))
    

URL = "http://localhost:8989"


class BADClient:
    def __init__(self):
        self.userName = ''
        self.email = ''
        self.password = ''
        self.userId = ""
        self.accessToken = ""
        self.subscriptions = []
        self.http_client = tornado.httpclient.HTTPClient()

        self.rqthread = None
        self.rqchannel = None

    def runRabbitQM(self, userId, callback, host='localhost'):
        def rabbitRun(host, userId):
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.rqchannel = connection.channel()

            self.rqchannel.queue_declare(queue=userId)

            self.rqchannel.basic_consume(callback,
                                  queue=userId,
                                  no_ack=True)

            print('[*] Waiting for messages. To exit press CTRL+C')
            self.rqchannel.start_consuming()

        self.rqthread = threading.Thread(target=rabbitRun, args=(host, userId))
        self.rqthread.start()

    def register(self, userName, email, password):
        print('Register')
        post_data = {'userName' : userName, 'email': email, 'password': password}
        response = service_call(URL, "register", post_data)

        if response:
            if response['status'] != 'success':
                print('Error:', response['error'])
            else:
                self.userId = response['userId']
                print(self.userName, 'Register', json.dumps(response))

    def login(self, userName, password):
        print('Login')
        post_data = {'userName' : userName, 'password': password}
        response = service_call(URL, "login", post_data)

        if response:
            if response['status'] != 'success':
                print('Error:', response['error'])
            else:
                self.userId = response['userId']
                self.accessToken = response['accessToken']
                print(self.userName, 'Login', json.dumps(response))

                self.runRabbitQM(self.userId, self.onNotifiedFromBroker, host='localhost')

    def subscribe(self, channelName, parameters): 
        print('Subscribe')
        post_data = {'userId' : self.userId, 'accessToken': self.accessToken,
            'channelName': channelName, 'parameters' : parameters}
        response = service_call(URL, "subscribe", post_data)
        
        if response:
            if response['status'] != 'success':
                print('Error:', response['error'])
            else:
                subscriptionId = response['subscriptionId']
                timestamp = response['timestamp']
                self.subscriptions.append({'subscriptionId': subscriptionId, 'timestamp': timestamp,
                        'channelName': channelName, 'parameters': parameters})
                print(self.userName, 'Subscribe', json.dumps(response))

    def getresults(self, channelName, subscriptionId, timestamp, recordCount):
        print(self.userName, 'Getresults for %s....' %(subscriptionId))
        
        try:
            post_data = {'userId': self.userId,
                         'accessToken': self.accessToken,
                         'channelName': channelName,
                         'subscriptionId': subscriptionId,
                         'timestamp': timestamp,
                         'maxRecords': recordCount
                         }

            request = tornado.httpclient.HTTPRequest(URL + "/" + 'getresults', 
                    method = 'POST',
                    body = json.dumps(post_data), 
                    request_timeout = 10)
            
            response =  self.http_client.fetch(request)
            
            if response:
                if response.error:
                    print(self.userName, 'GetresultsError', str(response.error))
                else:
                    print(self.userName, 'Getresults', response.body)

        except tornado.httpclient.HTTPError as e:
            print('Error:', str(e))
        except Exception as e:
            print('Error:', str(e))
    
    def onNotifiedFromBroker(self, channel, method, properties, body):
        print('Notified from broker', body)
        response = json.loads(body)

        channelName = response['channelName']
        subscriptionId = response['subscriptionId']
        recordCount = response['recordCount']
        timestamp = response['timestamp']

        self.getresults(channelName, subscriptionId, timestamp, recordCount)

        print(self.userName, 'GetResults for channel', channelName, 'subid', subscriptionId, 'records', recordCount, \
            'timestamp', timestamp)

    def onNewResultsOnChannel(self, results):
        print(self.userName, 'Channel results', results)

    def run(self):
        with open('client-commands.txt') as f:
            for line in f.readlines():
                if line.startswith("#") or len(line) <= 1:
                    continue
                print(line)
                command = json.loads(line)
                if command['command'] == 'register':
                    self.userName = command['userName']
                    self.email = command['email']
                    self.password = command['password']
                    self.register(self.userName, self.email, self.password)

                elif command['command'] == 'login':
                    self.login(self.userName, self.password)

                elif command['command'] == 'subscribe':
                    self.subscribe(command['channelName'], command['parameters'])

        if self.rqthread:
            print('Waiting for the child thread...')
            try:
                while self.rqthread.isAlive():
                    self.rqthread.join(10)
            except KeyboardInterrupt as error:
                self.rqchannel.cancel()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.rqchannel:
            self.rqchannel.cancel()

if __name__ == "__main__":
    BADClient().run()
