#!/usr/bin/env python3

import tornado.ioloop
import tornado.web
import tornado.httpclient

import socket
import hashlib
import simplejson as json
import sys
from datetime import datetime
from BADBroker import BADBroker
from asterixapi import AsterixQueryManager

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("This is BAD broker!!")


class RegistrationHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker
        
    def post(self):        
        print(str(self.request.body, encoding='utf-8'))
        post_data = json.loads(str(self.request.body, encoding='utf-8'))

        userName = post_data['userName']
        email = post_data['email']
        password = post_data['password']

        response = self.broker.register(userName, email, password)

        self.write(json.dumps(response))
        self.flush()
        self.finish()


class LoginHandler (tornado.web.RequestHandler):

    def initialize(self, broker):
        self.broker = broker

    def post(self):
        print(str(self.request.body, encoding='utf-8'))
        post_data = json.loads(str(self.request.body, encoding='utf-8'))

        userName = post_data['userName']
        password = post_data['password']

        response = self.broker.login(userName, password)

        self.write(json.dumps(response))
        self.flush()
        self.finish()


class SubscriptionHandler(tornado.web.RequestHandler):

    def initialize(self, broker):
        self.broker = broker

    def post(self):
        print(str(self.request.body, encoding='utf-8'))
        post_data = json.loads(str(self.request.body, encoding='utf-8'))

        userId = post_data['userId']
        accessToken = post_data['accessToken']
        channelName = post_data['channelName']
        parameters = post_data['parameters']

        response = self.broker.subscribe(userId, accessToken, channelName, parameters)

        self.write(json.dumps(response))
        self.flush()
        self.finish()


class GetResultsHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    def get(self):
        print(self.request.body)

    def post(self):
        print(str(self.request.body, encoding='utf-8'))
        post_data = json.loads(str(self.request.body, encoding='utf-8'))

        userId = post_data['userId']
        accessToken = post_data['accessToken']
        channelName = post_data['channelName']
        subscriptionId = post_data['subscriptionId']
        timestamp = post_data['timestamp']
        maxRecords = post_data['maxRecords']

        response = self.broker.getresults(userId, accessToken, channelName, subscriptionId, timestamp, maxRecords)

        print(json.dumps(response))
        self.write(json.dumps(response))
        self.flush()
        self.finish()

    def onNewResults(self, response):
        print(json.dumps(response))
        self.write(json.dumps(response))
        self.flush()
        self.finish()


class NotifyBrokerHandler(tornado.web.RequestHandler):

    def initialize(self, broker):
        self.broker = broker

    def get(self):
        print(str(self.request.body, encoding='utf-8'))

    def post(self):
        print('Broker received notifybroker')
        print(str(self.request.body, encoding='utf-8'))
                
        post_data = json.loads(self.request.body)
        print(post_data)
        
        brokerName = post_data['brokerName']
        channelName = post_data['channelName']
        subscriptions = post_data['subscriptions']
        
        response = self.broker.notifyBroker(brokerName, channelName, subscriptions)
        
        self.write(json.dumps(response))
        self.flush()
        self.finish()                


def start_server():
    asterix_backend = AsterixQueryManager("http://cacofonix-2.ics.uci.edu:19002");
    asterix_backend.setDataverseName("channels") # emergencyTest
    broker = BADBroker(asterix_backend)       				

    application = tornado.web.Application([
        (r"/", MainHandler),
        (r"/register", RegistrationHandler, dict(broker = broker)), 
        (r"/login", LoginHandler, dict(broker = broker)), 
        (r"/subscribe", SubscriptionHandler, dict(broker = broker)),
        (r"/getresults", GetResultsHandler, dict(broker = broker)), 
        (r"/notifybroker", NotifyBrokerHandler, dict(broker = broker)) 
    ])
    
    application.listen(8989)
    tornado.ioloop.IOLoop.current().start()
    
if __name__ == "__main__":
    start_server()
    

