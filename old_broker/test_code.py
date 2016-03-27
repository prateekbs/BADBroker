import json

class BADObject:
    def __init__(self, objectId):
        self.objectId = objectId
    
    def load(self):
        cmd_stmt = "for $t in dataset " + str(self.__class__.__name__) + "Dataset "
        cmd_stmt = cmd_stmt + " where $t.objectId = " + str(self.objectId) + " return $t"
        print cmd_stmt
        
        
    def save(self):        
        cmd_stmt = "upsert into dataset " + self.__class__.__name__ + "Dataset"
        cmd_stmt = cmd_stmt + "("
        cmd_stmt = cmd_stmt + json.dumps(self.__dict__)
        cmd_stmt = cmd_stmt + ")"
        print cmd_stmt

class User(BADObject):
    def __init__(self, userId):
        BADObject.__init__(self, userId)
        self.username = 'wewew'
        self.age = 2323
        self.password = 'fdffdffr'
    
        
                
user = User(3245566)
user.load()
user.save()
                
