#!/usr/bin/env python3
from itertools import chain

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
import redis
import re
import logging as log
import BADCache

log.getLogger(__name__)
log.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=log.INFO)

host = 'http://localhost:19002'

asterix_backend = AsterixQueryManager(host)
asterix_backend.setDataverseName('channels')


class BADObject:
    @tornado.gen.coroutine
    def delete(self):
        global asterix_backend
        cmd_stmt = 'delete $t from dataset ' + str(self.__class__.__name__) + 'Dataset '
        cmd_stmt = cmd_stmt + ' where $t.recordId = \"{0}\"'.format(self.recordId)
        log.debug(cmd_stmt)

        status, response = yield asterix_backend.executeUpdate(cmd_stmt)
        if status == 200:
            log.info('Delete succeeded')
            return True
        else:
            log.error('Delete failed. Error ' + response)
            raise Exception('Delete failed ' + response)

    @tornado.gen.coroutine
    def save(self):
        global asterix_backend
        cmd_stmt = 'insert into dataset ' + self.__class__.__name__ + 'Dataset'
        cmd_stmt = cmd_stmt + '('
        cmd_stmt = cmd_stmt + json.dumps(self.__dict__)
        cmd_stmt = cmd_stmt + ')'
        log.debug('CMD_STATEMENT is: '+cmd_stmt)
        status, response = yield asterix_backend.executeUpdate(cmd_stmt)
        if status == 200:
            log.info('Object %s Id %s saved' % (self.__class__.__name__, self.recordId))
            return True
        else:
            log.error('Object save failed, Error ' + response)
            raise Exception('Object save failed ' + response)

    @tornado.gen.coroutine
    def update(self):
        yield self.delete()
        yield self.save()

    @classmethod
    @tornado.gen.coroutine
    def load(cls, objectName, **kwargs):
        condition = None
        if kwargs:
            for key, value in kwargs.items():
                if isinstance(value, str):
                    paramvalue = '\"{0}\"'.format(value)
                else:
                    paramvalue = value

                if condition is None:
                    condition = '$t.{0} = {1}'.format(key, paramvalue)
                else:
                    condition = condition + ' and $t.{0} = {1}'.format(key, paramvalue)
        else:
            log.warning('No argument is provided for load')
            return None

        dataset = objectName + 'Dataset'
        query = 'for $t in dataset {0} where {1} return $t'.format(dataset, condition)
        status, response = yield asterix_backend.executeQuery(query)

        if status == 200 and response:
            response = response.replace('\n', '').replace(' ', '')
            if len(response) > 0:
                return json.loads(response, encoding='utf-8')
            else:
                return None
        else:
            return None

    @classmethod
    def createFrom(cls, objects):
        if not objects:
            return None

        if isinstance(objects, list):
            instances = []
            for object in objects:
                instance = cls()
                if not object or not isinstance(object, dict):
                    log.error('Creating %s Invalid argument %s' % (cls.__name__, object))
                    return None

                instance.__dict__ = object
                instances.append(instance)
            return instances
        else:
            object = objects
            if not isinstance(object, dict):
                log.error('Creating %s Invalid argument %s' % (cls.__name__, object))
                return None

            instance = cls()
            instance.__dict__ = object
            return instance


class User(BADObject):
    def __init__(self, recordId=None, userId=None, userName=None, password=None, email=None):
        self.recordId = recordId
        self.userId = userId
        self.userName = userName
        self.password = password        
        self.email = email

    @classmethod
    @tornado.gen.coroutine
    def load(cls, userName=None):
        objects = yield BADObject.load(cls.__name__, userName=userName)
        return User.createFrom(objects)

    def __str__(self):
        return self.userName + ' ID ' + self.userId


class ChannelSubscription(BADObject):
    def __init__(self, recordId=None, channelName=None, parameters=None, channelSubscriptionId=None):
        self.recordId = recordId
        self.channelName = channelName
        self.parameters = parameters
        self.channelSubscriptionId = channelSubscriptionId

    @classmethod
    @tornado.gen.coroutine
    def load(cls, channelName=None, parameters=None):
        objects = yield BADObject.load(cls.__name__, channelName=channelName, parameters=parameters)
        return ChannelSubscription.createFrom(objects)

class UserSubscription(BADObject):
    def __init__(self, recordId=None, userSubscriptionId=None, userId=None, channelSubscriptionId=None,
                 channelName=None, timestamp=None, resultsDataset=None):
        self.recordId = recordId
        self.userSubscriptionId = userSubscriptionId
        self.userId = userId
        self.channelSubscriptionId = channelSubscriptionId
        self.channelName = channelName
        self.timestamp = timestamp
        self.lastResultTimestamp = timestamp
        self.resultsDataset = resultsDataset

    def __str__(self):
        return self.userSubscriptionId

    def __repr__(self):
        return self.userSubscriptionId

    @classmethod
    @tornado.gen.coroutine
    def load(cls, userId=None, userSubscriptionId=None):
        if userId:
            objects = yield BADObject.load(cls.__name__, userId=userId)
        elif userSubscriptionId:
            objects = yield BADObject.load(cls.__name__, userSubscriptionId=userSubscriptionId)
        else:
            return None

        return UserSubscription.createFrom(objects)

class BADException(Exception):
    pass


class BADBroker:    
    def __init__(self,brokerName):
        global asterix_backend
        self.asterix_backend = asterix_backend
        self.brokerName = brokerName
        # self._myNetAddress()  # str(hashlib.sha224(self._myNetAddress()).hexdigest())
        self.users = {}
        
        self.subscriptions = {}  # susbscription indexed by channelName -> channelSubscriptionId-> userId
        self.userToSubscriptionMap = {}  # indexed by userSubscriptionId
        
        self.channelLastResultDeliveryTime = {}

        self.accessTokens = {}
        self.rabbitMQ = rabbitmq.RabbitMQ()
        self.cache = BADCache.BADLruCache()

    def _myNetAddress(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 0))  
        mylocaladdr = str(s.getsockname()[0])                
        return mylocaladdr

    @tornado.gen.coroutine
    def loadUser333(self, userName):
        query = 'for $t in dataset UserDataset where $t.userName =\"{0}\" return $t'.format(userName)
        status, response = yield asterix_backend.executeQuery(query)
        if status == 200:
            response = json.loads(response)
            print(response)
            if len(response) > 0:
                user = response[0]
                userId = user['userId']
                userName = user['userName']
                email = user['email']
                password = user['password']
                user = User(userId, userId, userName, password, email)
                return user
            else:
                return None
        else:
            return None

    @tornado.gen.coroutine
    def register(self, userName, email, password):
        users = yield User.load(userName=userName)
        if users and len(users) > 0:
            user = users[0]
            self.users[userName] = user
            log.warning('User %s is already registered' % (user.userId))
            return {'status': 'failed', 'error': 'User is already registered with the same name!',
                    'userId': user.userId}
        else:
            userId = userName  # str(hashlib.sha224(userName.encode()).hexdigest())
            user = User(userId, userId, userName, password, email)
            yield user.save()
            self.users[userName] = user
            log.debug('Registered user %s with id %s' % (userName, userId))
            return {'status': 'success', 'userId': userId}

    @tornado.gen.coroutine
    def login(self, userName, password):
        # user = yield self.loadUser(userName)
        users = yield User.load(userName=userName)

        if users and len(users) > 0:
            user = users[0]
            print(user.userName, user.password, password)
            if password == user.password:
                accessToken = str(hashlib.sha224((userName + str(datetime.now())).encode()).hexdigest())
                userId = user.userId
                self.accessTokens[userId] = accessToken

                tornado.ioloop.IOLoop.current().add_callback(self.loadSubscriptionsForUser, userId=userId)
                return {'status': 'success', 'userId': userId, 'accessToken': accessToken}
            else:
                return {'status': 'failed', 'error': 'Password does not match!'}
        else:
            return {'status': 'failed', 'error': 'No user exists %s!' % (userName)}

    @tornado.gen.coroutine
    def loadSubscriptionsForUser(self, userId):
        subscriptions = yield UserSubscription.load(userId=userId)
        return subscriptions

    @tornado.gen.coroutine
    def logoff(self, userId):
        if userId in self.accessTokens:
            del self.accessTokens[userId]
        
        return {'status': 'success', 'userId': userId}

    @tornado.gen.coroutine
    def createChannelSubscription(self, channelName, parameters):
        result = yield self.getChannelInfo(channelName)

        if result['status'] == 'failed':
            raise BADException('Retrieving channel info failed')

        channels = result['channels']

        log.info(channels)

        if len(channels) == 0:
            log.warning('No channel exits with name %s' % channelName)
            raise BADException('No channel exists with name %s!' % channelName)

        channel = channels[0]

        log.info(channel['Function'])
        log.info(channel['ChannelName'])
        log.info(channel['ResultsDatasetName'])

        function_name = channel['Function']
        arity = int(function_name.split('@')[1])

        if arity != len(parameters):
            log.warning('Channel routine takes %d arguments, given %d' % (arity, len(parameters)))
            raise BADException('Channel routine takes %d arguments, given %d' % (arity, len(parameters)))

        parameter_list = ''
        if parameters is not None:
            for value in parameters:
                if len(parameter_list) != 0:
                    parameter_list = parameter_list + ', '

                if isinstance(value, str):
                    parameter_list = parameter_list + '\"' + value + '\"'
                else:
                    parameter_list = parameter_list + str(value)

        aql_stmt = 'subscribe to ' + channelName + '(' + parameter_list + ') on ' + self.brokerName
        log.debug('AQL Statement: '+aql_stmt)
    
        status_code, response = yield self.asterix_backend.executeAQL(aql_stmt)

        if status_code != 200:
            raise BADException(response)

        response = response.replace('\n', '').replace(' ', '')
        log.debug(response)

        # response = json.loads(response)
        channelSubscriptionId = re.match(r'\[uuid\(\"(.*)\"\)\]', response).group(1)

        uniqueId = channelName + '::' + channelSubscriptionId
        channelSubscription = ChannelSubscription(uniqueId, channelName, str(parameters), channelSubscriptionId)
        yield channelSubscription.save()

        return channelSubscriptionId

    @tornado.gen.coroutine
    def createUserSubscription(self, userId, channelName, channelSubscriptionId, timestamp):
        resultsDataset = channelName + 'Results'

        userSubscriptionId = self.makeUserSubscriptionId(channelName, channelSubscriptionId, userId, timestamp)

        userSubscription = UserSubscription(userSubscriptionId, userSubscriptionId, userId,
                                    channelSubscriptionId, channelName, timestamp, resultsDataset)
        yield userSubscription.save()

        if channelName not in self.subscriptions:
            self.subscriptions[channelName] = {}

        if channelSubscriptionId not in self.subscriptions[channelName]:
            self.subscriptions[channelName][channelSubscriptionId] = {}

        log.info(self.subscriptions)
        self.subscriptions[channelName][channelSubscriptionId][userId] = userSubscription

        self.userToSubscriptionMap[userSubscriptionId] = userSubscription
        return userSubscriptionId

    @tornado.gen.coroutine
    def checkExistingChannelSubscription(self, channelName, parameters):
        channelSubscriptions = yield ChannelSubscription.load(channelName=channelName, parameters=str(parameters))
        if channelSubscriptions and len(channelSubscriptions):
            if len(channelSubscriptions) > 0:
                log.debug('Mutiple subscriptions matach, picking 0-th')
            return channelSubscriptions[0].channelSubscriptionId
        else:
            return None

    @tornado.gen.coroutine
    def subscribe(self, userId, accessToken, channelName, parameters):
        check = self._checkAccess(userId, accessToken)
        if check['status'] == 'failed':
            return check

        # Check whether the channel already has a subscription with same param values
        try:
            channelSubscriptionId = yield self.checkExistingChannelSubscription(channelName=channelName, parameters=parameters)
            if channelSubscriptionId:
                log.info('Subscription found, id = %s, channel %s with params %s' % (channelSubscriptionId, channelName, parameters))
            else:
                log.debug('Creating subscription, for channel %s with params %s' % (channelName, parameters))
                channelSubscriptionId = yield self.createChannelSubscription(channelName, parameters)
        except BADException as badex:
            log.error(badex)
            return {'status': 'failed', 'error': str(badex)}

        status, response = yield asterix_backend.executeQuery("let $t := current-datetime() return $t")
        if status != 200:
            return {'status': 'failed', 'error': response}

        log.debug(response)

        currentDateTime = json.loads(response)[0]  # re.match(r'\[\"(.*)\"\]', response).group(1)

        userSuscriptionId = yield self.createUserSubscription(userId, channelName, channelSubscriptionId, currentDateTime)

        if userSuscriptionId:
            return {'status': 'success', 'userSubscriptionId': userSuscriptionId, 'timestamp': currentDateTime}
        else:
            return {'status': 'failed', 'error': 'User subscription creation failed!!'}

    @tornado.gen.coroutine
    def unsubscribe(self, userId, accessToken, userSubscriptionId):
        check = self._checkAccess(userId, accessToken)
        if check['status'] == 'failed':
            return check

        if userSubscriptionId in self.userToSubscriptionMap:
            userSubscription = self.userToSubscriptionMap[userSubscriptionId]
            channelSubscriptionId = userSubscription.channelSubscriptionId
            channelName = userSubscription.channelName

            yield userSubscription.delete()

            del self.subscriptions[channelName][channelSubscriptionId][userId]
            del self.userToSubscriptionMap[userSubscriptionId]

            log.info('User %s unsubscribed from %s' % (userId, channelName))
            return {'status': 'success'}
        else:
            log.warning('No such subscription %s' % userSubscriptionId)
            return {'status': 'failed', 'error': 'No such subscription %s' % userSubscriptionId}

    def makeUserSubscriptionId(self, channelName, subscriptionId, userId, timestamp):
        return channelName + '::' + subscriptionId + '::' + userId + "@" + timestamp

    @tornado.gen.coroutine
    def getresults(self, userId, accessToken, userSubscriptionId, deliveryTime):
        check = self._checkAccess(userId, accessToken)
        if check['status'] == 'failed':
            return check

        if userSubscriptionId not in self.userToSubscriptionMap:
            msg = 'No subscription %s is found for user %s' % (userSubscriptionId, userId)
            log.warning(msg)
            return {'status': 'failed', 'error': msg}

        channelName = self.userToSubscriptionMap[userSubscriptionId].channelName
        channelSubscriptionId = self.userToSubscriptionMap[userSubscriptionId].channelSubscriptionId

        # if not deliveryTime:
        #    deliveryTime = self.subscriptions[channelName][subscriptionId][userId].lastResultTimestamp

        # Get results
        # First check in the cache, if not retrieve from Asterix store

        resultToUser = self.getResultsFromCache(channelName, channelSubscriptionId, deliveryTime)

        if resultToUser:
            log.info('Cache HIT for %s' % (self.getResultKey(channelName, channelSubscriptionId, deliveryTime)))
            log.debug(resultToUser)
        else:
            log.info('Cache MISS for %s' % (self.getResultKey(channelName, channelSubscriptionId, deliveryTime)))
            results = yield self.getResultsFromAsterix(channelName, channelSubscriptionId, deliveryTime)

            # Cache the results
            self.putResultsIntoCache(channelName, channelSubscriptionId, deliveryTime, resultToUser)

        # Update last delivery timestamp of this subscription
        subscription = self.subscriptions[channelName][channelSubscriptionId][userId]
        subscription.lastResultTimestamp = deliveryTime
        yield subscription.update()

        return {'status': 'success',
                'channelName': channelName,
                'userSubscriptionId': userSubscriptionId,
                'deliveryTime': deliveryTime,
                'results': resultToUser}


    def getResultKey(self, channelName, channelSubscriptionId, deliveryTime):
        return channelName + '::' + channelSubscriptionId + '::' + deliveryTime

    def getResultsFromCache(self, channelName, channelSubscriptionId, deliveryTime):
        resultKey = self.getResultKey(channelName, channelSubscriptionId, deliveryTime)
        cachedResults = self.cache.get(resultKey)

        if cachedResults:
            if isinstance(cachedResults, str) or isinstance(cachedResults, bytes):
                return json.loads(str(cachedResults, encoding='utf-8'))
            else:
                return cachedResults
        else:
            return None

    def putResultsIntoCache(self, channelName, channelSubscriptionId, deliveryTime, results):
        resultKey = self.getResultKey(channelName, channelSubscriptionId, deliveryTime)

        if self.cache.put(resultKey, results):
            log.info('Results %s cached' % resultKey)
        else:
            log.warning('Results %s caching failed' % resultKey)

    @tornado.gen.coroutine
    def getResultsFromAsterix(self, channelName, channelSubscriptionId, deliveryTime):
        #return [{'deliveryTime': deliveryTime, 'result': ['A', 'B', 'C']}]

        whereClause = '$t.subscriptionId = uuid(\"{0}\") ' \
                      'and $t.deliveryTime = datetime(\"{1}\")'.format(channelSubscriptionId, deliveryTime)
        orderbyClause = '$t.deliveryTime asc'
        aql_stmt = 'for $t in dataset %s where %s order by %s return $t' \
                   % ((channelName + 'Results'), whereClause, orderbyClause)

        status, response = yield self.asterix_backend.executeQuery(aql_stmt)

        log.debug('Results status %d response %s' % (status, response))

        if status == 200:
            if response:
                response = response.replace('\n', '')
                results = json.loads(response)

                resultToUser = []
                for item in results:
                    resultToUser.append(item['result'])
                return resultToUser
            else:
                return None
        else:
            return None

    @tornado.gen.coroutine
    def listchannels(self, userId, accessToken):
        check = self._checkAccess(userId, accessToken)
        if check['status'] == 'failed':
            return check
        
        aql_stmt = 'for $channel in dataset Metadata.Channel return $channel'
        status, response = yield self.asterix_backend.executeQuery(aql_stmt)
        
        if status == 200:
            response = response.replace('\n', '')
            print(response)
            
            channels = json.loads(str(response, encoding='utf-8'))

            return {'status': 'success', 'channels': channels}    
        else:
            return {'status': 'failed', 'error': response}

    @tornado.gen.coroutine
    def getChannelInfo(self, channelName):                
        aql_stmt = 'for $t in dataset Metadata.Channel '
        aql_stmt = aql_stmt + 'where $t.ChannelName = \"' + channelName + '\" '
        aql_stmt = aql_stmt + 'return $t'
        
        log.debug(aql_stmt)
        
        status, response = yield self.asterix_backend.executeQuery(aql_stmt)
        
        if status == 200:
            response = response.replace('\n', '')
            print(response)
            channels = json.loads(response, encoding='utf-8')
            return {'status': 'success', 'channels': channels}
        else:
            return {'status': 'failed', 'error': response}
    
    @tornado.gen.coroutine
    def notifyBroker(self, brokerName, dataverseName, channelName, subscriptions):
        # if brokerName != self.brokerName:
        #    return {'status': 'failed', 'error': 'Not the intended broker %s' %(brokerName)}

        # Retrieve results for this notification and notify all users
        #yield self.retrieveLastestResultsAndNotifyUsers(channelName)
        tornado.ioloop.IOLoop.current().add_callback(self.retrieveLastestResultsAndNotifyUsers, channelName=channelName)
        return {'status': 'success'}

    @tornado.gen.coroutine
    def retrieveLastestResultsAndNotifyUsers(self, channelName):
        log.debug('Current subscriptions: %s' % self.subscriptions)
        channelResultsName=channelName+'Results'
        # Retrieve the lastest delivery time for this channel
        if channelName in self.channelLastResultDeliveryTime:
            query = 'let $times := for $t in dataset '+ channelResultsName + \
                    ' where $t.deliveryTime > datetime(\"{0}\") ' \
                    'return $t.deliveryTime\n max($times)'.format(self.channelLastResultDeliveryTime[channelName])
        else:
            query = 'let $times := for $t in dataset allEmergenciesChannelResults ' \
                    'return $t.deliveryTime\n return max($times)'

        status, response = yield self.asterix_backend.executeQuery(query)

        lastestDeliveryTime = None
        if status == 200 and response:
            log.debug(response)
            lastestDeliveryTime = json.loads(response)[0]
            if lastestDeliveryTime:
                log.info('Channel %s Latest delivery time %s' % (channelName, lastestDeliveryTime))
                if lastestDeliveryTime and channelName in self.subscriptions:
                    for channelSubscriptionId in self.subscriptions[channelName]:
                        results = yield self.getResultsFromAsterix(channelName, channelSubscriptionId, lastestDeliveryTime)
                        resultKey = self.getResultKey(channelName, channelSubscriptionId, lastestDeliveryTime)
                        if results and not self.cache.hasKey(resultKey):
                            self.putResultsIntoCache(channelName, channelSubscriptionId, lastestDeliveryTime, results)

                # Sending notification to all users subscribed to this channel
                tornado.ioloop.IOLoop.current().add_callback(self.notifyAllUsers, channelName=channelName, lastestDeliveryTime=lastestDeliveryTime)
            else:
                log.error('Retrieving delivery time failed for channel %s' % channelName)
        else:
            log.error('Retrieving delivery time failed for channel %s' % channelName)

    def notifyAllUsers(self, channelName, lastestDeliveryTime):
        log.info('Sending out notification for channel %s deliverytime %s' % (channelName, lastestDeliveryTime))

        if channelName in self.subscriptions:
            for channelSubscriptionId in self.subscriptions[channelName]:
                for userId in self.subscriptions[channelName][channelSubscriptionId]:
                    sub = self.subscriptions[channelName][channelSubscriptionId][userId]
                    userSubcriptionId = sub.userSubscriptionId
                    self.notifyUser(channelName, userId, userSubcriptionId, lastestDeliveryTime)


    def notifyUser(self, channelName, userId, userSubscriptionId, lastestDeliveryTime):
        log.info('Channel %s: sending notification to user %s for %s' % (channelName, userId, userSubscriptionId))

        message = {'userId': userId,
                   'channelName':  channelName,
                   'userSubscriptionId': userSubscriptionId,
                   'recordCount': 0,
                   'timestamp': lastestDeliveryTime
                   }

        self.rabbitMQ.sendMessage(userId, json.dumps(message))

    def _checkAccess(self, userId, accessToken):
        return {'status': 'success'}

        '''
        if userId in self.accessTokens:
            if accessToken == self.accessTokens[userId]:
                return { 'status': 'success' }
            else:
                return { 'status': 'failed',
                         'error': 'Invalid access token'}
        else: 
            return {'status': 'failed',
                    'error': 'User not authenticated'}
        '''

    def __del__(self):
        self.rabbitMQ.close()

    @tornado.gen.coroutine
    def setupBroker(self):
        commands = ''
        with open("1") as f:
            for line in f.readlines():
                if not line.startswith('#'):
                    commands = commands + line
        log.info('Executing commands: ' + commands)
        status, response = yield self.asterix_backend.executeAQL(commands)

        if status != 200:
            log.error('Broker setup failed ' + response)


