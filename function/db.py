#!/usr/bin/python3
import sys
import pymongo
from bson.objectid import ObjectId
from bson.json_util import loads, dumps
import json

#Config
host="localhost"
port = 27017
uname = "*"
pwd = "*"
db = "iot-server"
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

    def find(self, col, filter = None, exclude = None, limit = None, skip = None, sort=('$natural',1) ):
        (sort1,sort2) = sort
        if not self.checkCollections(col):
            return False
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
            document['id'] = str(document['_id'])
            del document['_id']
            response.append(document)
        return json.loads(dumps(response))

    def findOne(self, col, filter = None, exclude = None):
        if not self.checkCollections(col):
            return False
        self.col = self.db[col]
        #return json.loads(json.dumps(list(self.col.find(filter,exclude)),default=json_util.default))
        response = []
        res = self.col.find_one(filter,exclude)
        if res:                        
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
        if not self.checkCollections(col):
            return False
        self.col = self.db[col]
        x = self.col.delete_one(filter)
        if x.deleted_count > 0:
            return True
        return False

    def deleteDataMany(self,col,filter):
        if not self.checkCollections(col):
            return False
        self.col = self.db[col]
        x = self.col.delete_many(filter)
        if x.deleted_count > 0:
            return True
        return False

    def updateData(self,col,filter,values):
        if not self.checkCollections(col):
            return False
        self.col = self.db[col]
        x = self.col.update(filter,{ "$set": values })        
        if(x['nModified'] > 0):
            return True
        return False

    def updateDataOne(self,col,filter,values):
        if not self.checkCollections(col):
            return False
        self.col = self.db[col]
        x = self.col.update_one(filter,{ "$set": values })
        if(x['nModified'] > 0):
            return True
        return False

    def updatePush(self,col,filter,values):
        if not self.checkCollections(col):
            return False
        self.col = self.db[col]
        x = self.col.update(filter,{ "$push": values })        
        if(x['nModified'] > 0):
            return True
        return False

    def updatePull(self,col,filter,values):
        if not self.checkCollections(col):
            return False
        self.col = self.db[col]
        x = self.col.update(filter,{ "$pull": values })        
        if(x['nModified'] > 0):
            return True
        return False
        
    def renameField(self,col,filter,values):
        if not self.checkCollections(col):
            return False
        self.col = self.db[col]
        x = self.col.update_one(filter,{ "$rename": values })
        if(x['nModified'] > 0):
            return True
        return False
#        sys.stdout.flush()

if __name__ == "__main__":
    mongo = dbmongo()    