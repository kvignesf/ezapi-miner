# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************

import copy
import pymongo


def get_db_connection(dbname, host="localhost", port=27017):
    #client = pymongo.MongoClient(host, port)
    #client = pymongo.MongoClient("mongodb+srv://ezapimongoadmin:JRVvuh9D5V0IZxCW@cluster0.z8ggg.gcp.mongodb.net/test?authSource=admin&replicaSet=atlas-7cv8k4-shard-0&readPreference=primary&ssl=true", Connect=False)
    client = pymongo.MongoClient("mongodb://root:JRVvuh9D5V0IZxCW@35.193.104.91:27017/?authSource=admin&readPreference=primary", Connect=False)
    db = client[dbname]
    return (client, db)


def store_document(collection, document, db):
    document_copy = copy.deepcopy(document)
    db_collection = db[collection]
    db_collection.insert_one(document_copy)
