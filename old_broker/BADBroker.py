#!/usr/bin/env python3

import tornado.ioloop
import tornado.web
import tornado.gen
import tornado.httpclient

import socket
import hashlib
import simplejson as json

from datetime import datetime
from asterixapi import *
import rabbitmq

class BADObject:
    def __init__(self, objectId):
        self.objectId = objectId
    
    def load(self):
        cmd_stmt = "for $t in dataset " + str(self.__class__.__name__) + "Dataset "
        cmd_stmt = cmd_stmt + " where $t.objectId = " + str(self.objectId) + " return $t"
        print(cmd_stmt)
        
        
    def save(self):        
        cmd_stmt = "upsert into dataset " + self.__class__.__name__ + "Dataset"
        cmd_stmt = cmd_stmt + "("
        cmd_stmt = cmd_stmt + json.dumps(self.__dict__)
        cmd_stmt = cmd_stmt + ")"
        print(cmd_stmt)

class User(BADObject):
    def __init__(self, objectId):
        BADObject.__init__(self, objectId)
    
    def __init__(self, objectId, userId, userName, password, email):
        BADObject.__init__(self, objectId)
        self.userId = userId
        self.userName = userName
        self.password = password        
        self.email = email
     

class Subscription(BADObject):
    def __init__(self, objectId):
        BADObject.__init__(self, objectId)
    
    def __init__(self, objectId, subscriptionId, userId, channelName, timestamp, resultsDataset, maxrecords):  
        BADObject.__init__(self, objectId)
        self.subscriptionId = subscriptionId
        self.userId = userId
        self.channelName = channelName
        self.timestamp = timestamp
        self.resultsDataset = resultsDataset
        self.maxrecords = maxrecords

class BADJsonFields:
    status = "status"
    success = "success"
    failed = "failed"

    error = "error"
    userId = "userId"
    subscriptionId = "subscriptionId"
    accessToken = "accessToken"
    channelName = "ChannelName"
    results = "results"
    timestamp = "timestamp"
    channels = "channels"

class BADBroker:    
    def __init__(self, asterix_backend):
        self.asterix_backend = asterix_backend
        self.brokerName = "brokerA"  #self._myNetAddress()  # str(hashlib.sha224(self._myNetAddress()).hexdigest())
        self.users = {}        
        self.subscriptions = {}

        self.accessTokens = {}
        self.rabbitMQ = rabbitmq.RabbitMQ()

    def _myNetAddress(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 0))  
        mylocaladdr = str(s.getsockname()[0])                
        return mylocaladdr
        
    def register(self, userName, email, password):
        if userName in self.users:
            return {"status": "failed", "error": "User is already registered with the same name!", 
                    'userId': self.users[userName].userId}
        else:
            userId = userName #str(hashlib.sha224(userName.encode()).hexdigest())
            user = User(userId, userId, userName, password, email)
            user.save()
            self.users[userName] = user
            
        return {"status": "success", "userId": userId}
    
    def login(self, userName, password):
        if userName in self.users:
            if password == self.users[userName].password:
                accessToken = str(hashlib.sha224((userName + str(datetime.now())).encode()).hexdigest())
                userId = self.users[userName].userId 
                self.accessTokens[userId] = accessToken                               
                return {"status": "success", "userId": userId, "accessToken": accessToken}
        
        return {"status": "failed", "error": "Username or password does not match!"}            
            
    
    def logoff(self, userId):
        if userId in self.accessTokens:
            del self.accessTokens[userId]
        
        return {'status': 'success', 'userId': userId}

    def subscribe(self, userId, accessToken, channelName, parameters):    
        check = self._checkAccess(userId, accessToken)
        if check["status"] == "failed":
            return check
        
        result = self.getChannelInfo(channelName)                
        
        if result['status'] == 'failed':
            return result
        
        channels = result['channels']
        
        print( channels)
        
        if len(channels) == 0:
            return {'status': 'failed', 'error': 'No channel exists named %s!' %(channelName)}
            
        channel = channels[0]
        
        print( channel['Function'])
        print( channel['ChannelName'])
        print( channel['ResultsDatasetName'])
        
        function_name = channel['Function']
        arity = int(function_name.split("@")[1])
        results_dataset_name = channel['ResultsDatasetName']
        
        if arity != len(parameters):
            return { 'status': 'failed', 
                     'error': 'Channel routine take %d arguments, given %d' %(arity, len(parameters)) }

        parameter_list = ""
        if parameters is not None:
            for value in parameters:         
                if len(parameter_list) != 0:
                    parameter_list = parameter_list + ", "
                           
                if isinstance(value, str):
                    parameter_list = parameter_list + "\"" + value + "\""
                else:
                    parameter_list = parameter_list + str(value)

        aql_stmt = "subscribe to " + channelName + "(" + parameter_list + ") on " + self.brokerName
        print( aql_stmt)
        
        status_code, response = self.asterix_backend.executeAQL(aql_stmt)
        
        if status_code != 200:
            return {"status": "failed", 'error': response }

        print(response)

        try:
            response = json.loads(response)
        except Exception as e:
            print ('Error', str(e))
            #return { 'status': 'failed', 'error': 'Bad formatted response %s' %(response)}

        subscriptionId = 'sub1'
        timestamp = 0

        maxRecords = 10
        resultsDataset = results_dataset_name
        
        uniqueSubId = self.getUniqueSubId(channelName, subscriptionId)

        subscription = Subscription(uniqueSubId, subscriptionId, userId, channelName, timestamp,
                                        resultsDataset, maxRecords)
        subscription.save()
        
        self.subscriptions[uniqueSubId] = subscription
                                     
        return {'status': 'success', 'subscriptionId': subscriptionId, 'timestamp': timestamp}
        
    def getUniqueSubId(self, channelName, subscriptionId):
        return channelName + '::' + subscriptionId

    def getresults(self, userId, accessToken, channelName, subscriptionId, timestamp, maxRecords):
        check = self._checkAccess(userId, accessToken)
        if check["status"] == "failed":
            return check

        uniqueSubId = self.getUniqueSubId(channelName, subscriptionId)

        if uniqueSubId not in self.subscriptions:
            return {'status': 'failed', 'error': 'No such subscription'}

        if self.subscriptions[uniqueSubId].userId != userId:
            return {'status': 'failed', 'error': 'Not a valid subscription for user %s' %(userId)}
        
        
        # Get results
        results = self.getResultsFromAsterix(channelName, subscriptionId, timestamp, maxRecords)
        
        return {'status': 'success', 'subscriptionId': subscriptionId, 'Results': results}
        
        """
        if len(results) > 0:            
            return {"status": "success", 'subscriptionId': subscriptionId, 'Results': results} 
        else:
            if requestHandler is not None:
                print( 'Registering callback for', subscriptionId
                self.callbacks[subscriptionId] = requestHandler.onNewResults
                return {'status': 'wait', 'subscriptionId': subscriptionId}
            else:
                return {'status': 'success', 'subscriptionId': subscriptionId, 'Results': results}    

        """    

    def getResultsFromAsterix(self, channelName, subscriptionId, timestamp, maxRecords):
        #results = [dict(key1 = "value2", key2 = "value2"), dict(key1=12, key2 = 234)]
        aql_stmt = 'for $t in dataset %s return $t' %(channelName + "Results")
        results = self.asterix_backend.executeQuery(aql_stmt)
        return results 
    
    
    def listchannels(self, userId, accessToken):
        check = self._checkAccess(userId, accessToken)
        if check["status"] == "failed":
            return check
        
        aql_stmt = "for $channel in dataset Metadata.Channel return $channel"
        status, response = self.asterix_backend.executeQuery(aql_stmt)
        
        if status == 200:
            response = response.replace('\n', '')
            print( response)
            
            channels = json.loads(response)
            return {"status": "success", "channels": channels}    
        else:
            return {"status": "failed", "error": response}
    
    
    def getChannelInfo(self, channelName):                
        aql_stmt = "for $t in dataset Metadata.Channel "
        aql_stmt = aql_stmt + "where $t.ChannelName = \"" + channelName + "\" "
        aql_stmt = aql_stmt + "return $t"
        
        print( aql_stmt)
        
        status, response = self.asterix_backend.executeQuery(aql_stmt)
        
        if status == 200:
            response = response.replace('\n', '')
            print( response)
            channels = json.loads(response)
            return {'status': 'success', 'channels': channels}
        else:
            print( {'status': 'failed', 'error': response})
    
    
    def notifyBroker(self, brokerName, channelName, subscriptions):
        #if brokerName != self.brokerName:
        #    return {'status': 'failed', 'error': 'Not the intended broker %s' %(brokerName)}
        
        result = self.getChannelInfo(channelName)                        
        if result['status'] == 'failed':
            return result
        
        channels = result['channels']
        
        if len(channels) == 0:
            return {'status': 'failed', 'error': 'No channel exists named %s!' %(channelName)}
            
        channel = channels[0]
        
        print( 'Notification received for', [sub['subscriptionId'] for sub in subscriptions])
        
        for sub in subscriptions:
            print( 'Processing notification for', sub['subscriptionId'])
            subscriptionId = sub['subscriptionId']
            timestamp = sub['timestamp']
            recordCount = sub['recordCount']
            
            self.notifyUser(channelName, subscriptionId, timestamp, recordCount)
            
            '''
            if subscriptionId in self.callbacks:
                callback = self.callbacks[subscriptionId]
                if callback is not None:
                    print( 'Callbacks to', subscriptionId)
                    results = self.getResultsFromAsterix(subscriptionId, timestamp, recordcount)
                    response = {
                        'status': 'success',
                        'subscriptionId': subscriptionId,
                        'Results': results
                        }
                    
                    tornado.ioloop.IOLoop.current().add_callback(callback, response)
                    del self.callbacks[subscriptionId]                    
            '''
        return {'status': 'success'}
                       

    def notifyUser(self, channelName, subscriptionId, timestamp, recordCount):
        uniqueSubId = self.getUniqueSubId(channelName, subscriptionId)
        print( self.subscriptions)

        if uniqueSubId in self.subscriptions:
            userId = self.subscriptions[uniqueSubId].userId
            # Send message to this user
            # message contain channelname, subscriptionId, timestamp, recordcount

            message = { 'userId': userId,
                        'channelName':  channelName,
                        'subscriptionId': subscriptionId,
                        'recordCount': recordCount,
                        'timestamp': timestamp
                      }
            self.rabbitMQ.sendMessage(userId, json.dumps(message))

    def _checkAccess(self, userId, accessToken):
        if userId in self.accessTokens:
            if accessToken == self.accessTokens[userId]:
                return { "status": "success" }
            else:
                return { "status": "failed",
                         "error": "Invalid access token"}
        else: 
            return { "status": "failed",
                     "error": "User not authenticated"}
        

def test_broker():
    asterix_backend = AsterixQueryManager("http://cacofonix-2.ics.uci.edu:19002");
    #asterix_backend.setDataverseName("emergencyTest")
    asterix_backend.setDataverseName('channels')
    broker = BADBroker(asterix_backend)
    
    print( broker.register("sarwar", "ysar@gm.com", "pass"))
    
    result =  broker.login("sarwar", "pass")
    userId = result["userId"]
    accessToken = result["accessToken"]

    #print( broker.listchannels(userId, accessToken))
    #print( broker.getChannelInfo(userId, accessToken, "EmergencyMessagesChannel"))
    result = broker.subscribe(userId, accessToken, "nearbyTweetChannel", [12])
    
    subscriptionId = result['subscriptionId']
    print( broker.getresults(userId, accessToken, 'nearbyTweetChannel', subscriptionId, 12235, 100))

    print( broker.listchannels(userId, accessToken))
    
    print( broker.notifyBroker(broker.brokerName, 
                "nearbyTweetChannel", [{'subscriptionId': subscriptionId, 'timestamp': 121, 'recordCount': 12}]))
    
    #test = {'A': 12, 'B': [{"X": 12}, {"Y": 23}, {"Z": 34}]}
    #print( test['B'][0])



if __name__ == "__main__":
    test_broker()

