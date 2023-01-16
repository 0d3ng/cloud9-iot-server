#!/usr/bin/python3
import sys
import pymongo
from bson.objectid import ObjectId
from bson.json_util import loads, dumps
import json
from configparser import ConfigParser
import time
config = ConfigParser()
config.read("config.ini")
#Config
host    = config["DB"]["host"]
port    = int(config["DB"]["port"])
uname   = config["DB"]["uname"]
pwd     = config["DB"]["pwd"]
db      = config["DB"]["db"]
client = pymongo.MongoClient(host=host,port=port, authSource=db)
db = client[db]

class dbmongo:
    def __init__(self):
        self.client = client #username=uname, password=pwd,
        self.db = db

    def checkCollections(self, col):
        self.collist = self.db.list_collection_names()
        if col in self.collist:
            return True
        return False

    def find(self, col, filter = None, exclude = None, limit = None, skip = None, sort=('$natural',1), showID=None ):
        (sort1,sort2) = sort        
        # if not self.checkCollections(col):
        #     return []
        self.col = self.db[col] 
        if (limit is None) and (skip is None):
            res = self.col.find(filter,exclude).sort(sort1,sort2)            
        elif not ( (limit is None) or (skip is None) ):
            res = self.col.find(filter,exclude).limit(limit).skip(skip).sort(sort1,sort2)
        elif not (limit is None):
            res = self.col.find(filter,exclude).limit(limit).sort(sort1,sort2)
        elif not (skip is None):
            res = self.col.find(filter,exclude).skip(skip).sort(sort1,sort2)     
        response = []
        for document in res:
            if('id' in document or showID == True):
                document['_id'] = str(document['_id'])
            else:
                document['id'] = str(document['_id'])
                del document['_id']
            response.append(document)
        return json.loads(dumps(response))

    def findOne(self, col, filter = None, exclude = None, sort=('$natural',1), showID=None  ):
        # if not self.checkCollections(col):
        #     return False        
        self.col = self.db[col]
        #return json.loads(json.dumps(list(self.col.find(filter,exclude)),default=json_util.default))
        response = []
        res = self.col.find_one(filter,exclude,sort=[sort]) 
        if res:       
            if('id' in res or showID == True):
                res['_id'] = str(res['_id'])
            else:
                res['id'] = str(res['_id'])
                del res['_id']                
        return json.loads(dumps(res))

    def insertData(self,col,data):
        # if not self.checkCollections(col):
        #     return False
        self.col = self.db[col]
        x = self.col.insert_one(data)
        return x.inserted_id

    def deleteData(self,col,filter):
        # if not self.checkCollections(col):
        #     return False
        self.col = self.db[col]
        x = self.col.delete_one(filter)
        if x.deleted_count > 0:
            return True
        return False

    def deleteDataMany(self,col,filter):
        # if not self.checkCollections(col):
        #     return False
        self.col = self.db[col]
        x = self.col.delete_many(filter)
        if x.deleted_count > 0:
            return True
        return False

    def updateData(self,col,filter,values):
        # if not self.checkCollections(col):
        #     return False
        self.col = self.db[col]
        x = self.col.update(filter,{ "$set": values })        
        if(x['nModified'] > 0):
            return True
        return False

    def updateDataOne(self,col,filter,values):
        # if not self.checkCollections(col):
        #     return False
        self.col = self.db[col]
        x = self.col.update_one(filter,{ "$set": values })
        if(x['nModified'] > 0):
            return True
        return False

    def updatePush(self,col,filter,values):
        # if not self.checkCollections(col):
        #     return False
        self.col = self.db[col]
        x = self.col.update(filter,{ "$push": values })        
        if(x['nModified'] > 0):
            return True
        return False

    def updatePull(self,col,filter,values):
        # if not self.checkCollections(col):
        #     return False
        self.col = self.db[col]
        x = self.col.update(filter,{ "$pull": values })        
        if(x['nModified'] > 0):
            return True
        return False
        
    def renameField(self,col,filter,values):
        # if not self.checkCollections(col):
        #     return False
        self.col = self.db[col]
        x = self.col.update_one(filter,{ "$rename": values })
        if(x['nModified'] > 0):
            return True
        return False

    def count(self, col, filter = None, exclude = None, limit = None, skip = None, sort=('$natural',1) ):
        (sort1,sort2) = sort
        # if not self.checkCollections(col):
        #     return []
        self.col = self.db[col] 
        if (limit is None) and (skip is None):
            res = self.col.find(filter,exclude).sort(sort1,sort2).count()            
        elif not ( (limit is None) or (skip is None) ):
            res = self.col.find(filter,exclude).limit(limit).skip(skip).sort(sort1,sort2).count()
        elif not (limit is None):
            res = self.col.find(filter,exclude).limit(limit).sort(sort1,sort2).count()
        elif not (skip is None):
            res = self.col.find(filter,exclude).skip(skip).sort(sort1,sort2).count()     
        
        return res 

    def aggregate(self, col, pipeline = None):
        # if not self.checkCollections(col):
        #     return []
        self.col = self.db[col] 
        res = self.col.aggregate(pipeline)   
        response = []
        for document in res:
            if('id' in document):
                document['_id'] = str(document['_id'])
            else:
                document['id'] = str(document['_id'])
                del document['_id']
            response.append(document)
        return json.loads(dumps(response))

    # def find_aggre(self, col, filter = None, exclude = None, limit = None, skip = None, sort=('$natural',1) ):
    #     (sort1,sort2) = sort        
    #     self.col = self.db[col] 
    #     project = {"_id": { "$toString": "$_id" }}
    #     match = 
    #     if (limit is None) and (skip is None):
    #         res = self.col.find(filter,exclude).sort(sort1,sort2)            
    #     elif not ( (limit is None) or (skip is None) ):
    #         res = self.col.find(filter,exclude).limit(limit).skip(skip).sort(sort1,sort2)
    #     elif not (limit is None):
    #         res = self.col.find(filter,exclude).limit(limit).sort(sort1,sort2)
    #     elif not (skip is None):
    #         res = self.col.find(filter,exclude).skip(skip).sort(sort1,sort2)     
    #     response = []
    #     # for document in res:
    #     #     if('id' in document):
    #     #         document['_id'] = str(document['_id'])
    #     #     else:
    #     #         document['id'] = str(document['_id'])
    #     #         del document['_id']
    #     #     response.append(document)
    #     return json.loads(dumps(response))

if __name__ == "__main__":
    mongo = dbmongo()    

# t = time.time()
# elapsed_time = time.time() - t
# print("**Count***")
# print(col)
# print(elapsed_time)
# print("**********")