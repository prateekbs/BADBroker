
import requests
import json
import sys
from asterixapi import *
import time, threading
import hashlib

class PreparedStatement():
    def __init__(self, statement):
        self.statement = statement
        self._split()
        self.values = {}        
        
    def _split(self):
        self.segments = self.statement.split("?")
        
    def setValue(self, position, value):
        self.values[position] = value
        return self    
    
    def setValues(self, params):
        if params:
            for i in range(len(params)):
                self.setValue(i, params[i]) 
        return self

    def getStatement(self):
        stmt = ""     
        for i in range(len(self.segments)):
            stmt = stmt + (" " if len(stmt) > 0 else '') + self.segments[i]
            if i < len(self.segments) - 1:            
                if i in self.values:
                    value = "\"" + self.values[i] + "\"" if isinstance(self.values[i], str) else str(self.values[i]) 
                    stmt = stmt + " " + value
                else:
                    stmt = stmt + " ? "
    
        return stmt
        
    def __str__(self):
        return self.statement
    
    def __repr__(self):
        return self.statement
        

class Subscription():
    def __init__(self, channel, params, callback):
        self.channel = channel
        self.params = params
        self.interval = self.channel.interval
        
        self.callback = callback
        self.scheduler = None        
        
        self.astm = AsterixQueryManager("http://cacofonix-4.ics.uci.edu:19002")
        self.astm.setDataverseName(self.channel.dataverse)    
        self.subscriptionStatement = self.channel.channelStatement.setValues(params).getStatement()
    
        self.subscriptionID = hashlib.sha224(self.subscriptionStatement).hexdigest()
        
    def start(self):        
        self._fetchChannelResults()    
    
    def stop(self):
        print('Stopping subscription for channel %s!' %(self.channel))
        if self.scheduler:
            self.scheduler.cancel()        
            self.scheduler = None
            
    def _fetchChannelResults(self):
        print('Invoking channel function for channel %s' %(self.channel))
        print('Channel function %s' %(self.subscriptionStatement))
        
        status_code, results = self.astm.executeQuery(self.subscriptionStatement)
        print('Status:' , status_code)
        print('Results: ', results)
        
        if status_code == 200:            
            self.callback(self.subscriptionID, results)
        
        self.scheduler = threading.Timer(self.interval, self._fetchChannelResults)
        self.scheduler.start()        
    
    
    def __str__(self):
        return self.subcriptionID
        
    def __hash__(self):
        if self in None:
            return 0
        elif self.subscriptionID:        
            return self.subscriptionID.__hash__()
        else:
            return 0
                        
class Channel():
    def __init__(self, dataverse, name, channelStatement, interval):
        self.dataverse = dataverse
        self.name  = name
        self.channelStatement = channelStatement
        self.interval = interval
   
    def __str__(self):
        return self.name
    
    def __str__(self):
        return self.name           
    
def processResult(subId, results):
    print('Got result for %s' %(subId))
    sys.stdout.write(subId + ' ' + results + '\n\n\n')
    

def listOfChannels():
    return

        
#query = 'for $m in dataset CHPReportsDefault where $m.radius < ? return $m'        
#query = 'for $m in dataset CHPReportsDefault where contains($m.message, ?) return $m'        

query = 'for $m in MessageWithKeyword(?) return $m'        
#ps = PreparedStatement("for $m in MessageWithinRadius(?) return $m")

ps = PreparedStatement(query)
channel1 = Channel('emergencyTest', "message_within_radius", ps, 2)
sub = Subscription(channel1, ["Evacuate"], processResult)

sub.start()
time.sleep(10)
sub.stop()

sys.exit(0)

            
"""
request_url = "http://127.0.0.1:19002/query"
query = "use dataverse TinySocial; for $user in dataset FacebookUsers where $user.id = 8 return $user;"

response = requests.get(request_url, params = {"query" : query})

#response = requests.get(request_url, params = {"query" : query, "mode": "asynchronous"})
#response = requests.get(request_url +"/result", params = {"handle" : "\"handle\":\"[59, 0]\""})

print(response.url)
print(response.status_code)
print(response.text)
"""

manager = AsterixQueryManager("http://cacofonix-4.ics.uci.edu:19002", "query");
#manager.setDataverseName("TinySocial")
#manager.forClause("$user in dataset FacebookUsers").whereClause("$user.id = 8").returnClause("$user");
manager.setDataverseName("emergencyTest")
manager.forClause("$x in dataset CHPReportsDefault").returnClause("$x");
status_code, response = manager.execute();

print(status_code)
print(response)

objects =  json.loads(response)

if isinstance(objects, list):
    for item in objects:
        print(item)
else:
    for key, value in objects:
        print(key, value)


manager.reset()
#status_code, result = manager.executeDDL("create function MessageWithinRadius($radius) {for $m in dataset CHPReportsDefault where $m.radius < $radius return $m}")            

myChannel = "for $m in dataset CHPReportsDefault where $m.radius < 20 return $m"
print(manager.executeQuery(myChannel))

sys.exit(0)

