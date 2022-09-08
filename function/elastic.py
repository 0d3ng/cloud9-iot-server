# # -*- coding: utf-8 -*-
# from elasticsearch import Elasticsearch

# class elastic:
#     def __init__(self,host = 'x1.hcm-lab.id',port = 9200):
#         self.client =  Elasticsearch(host = host, port = port)

#     def createIndex(self,index):
#         self.client.indices.create(index=index, ignore=400)

#     def insertOne(self,_index,_data='',_doc_type='authors',_id=''):
#         if(_id):
#             self.client.index(index=_index, doc_type=_doc_type, id=_id, body=_data)
#         else:
#             self.client.index(index=_index, doc_type=_doc_type, body=_data)
            