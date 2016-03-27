#!/usr/bin/env python3
import simplejson as json
import redis
import sys


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
    def __init__(self, userId):
        BADObject.__init__(self, userId)
        self.username = 'wewew'
        self.age = 2323
        self.password = 'fdffdffr'
    
        
def test():
    yield [1, 2]
    yield [3, 4]
    yield [4, 6]

for x in test():
    print(x)

user = User(3245566)
user.load()
user.save()

x = {'a' : 1, 'b': 2, 'c': {'t1': 1, 't2': 12}}
for item in x:
    print (item)

for item in sorted(x.keys(), reverse=True):
    print (item)

for k, v in x.items():
    print (k, v)


class User():
    def __init__(self):
        self.__dict__ = {'userName': 'abc', 'age': 12}


u = User()

print(u.userName, u.age)

sys.exit(0)





r = redis.StrictRedis(host='localhost', port=6379, db=0)

y = {'b': 2, 'c': {'x': 10, 'z': 12, 'y': 11}, 'a': 1}
json.dumps(y)

r.set('mydict', json.dumps(y))

mydict = r.get('mydict')
print(mydict)

mydict = json.loads(str(mydict, encoding='utf-8'))
print(mydict)

print(r.exists('mydict'))
r.publish('first', 'Hello')