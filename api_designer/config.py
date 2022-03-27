# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************


import copy
import pymongo
from decouple import config
import datetime, decimal

def json_safe(obj):
    if type(obj) in (datetime.datetime, datetime.date, datetime.time):
        return obj.isoformat()
    elif type(obj) == decimal.Decimal:
        return float(obj)
    elif type(obj) == dict:
        for (k, v) in obj.items():
            obj[k] = json_safe(v)
        return obj
    elif type(obj) == list:
        return [json_safe(li) for li in obj]
    elif type(obj) == memoryview:
        return obj.tobytes()
    return obj

def get_db_connection(dbname="ezapi", host="localhost", port=27017):
    #client = pymongo.MongoClient(host, port)
    #client = pymongo.MongoClient("mongodb://root:JRVvuh9D5V0IZxCW@34.66.45.162:27017/?authSource=admin",Connect=False)
    client = pymongo.MongoClient(
        config('dbconfig'),
        Connect=False,
    )
    db = client[dbname]
    return (client, db)


def store_document(collection, document, db):
    document_copy = copy.deepcopy(json_safe(document))
    db_collection = db[collection]
    db_collection.insert_one(document_copy)


def store_bulk_document(collection, document_list, db):
    document_list_copy = copy.deepcopy(json_safe(document_list))
    db_collection = db[collection]
    db_collection.insert_many(document_list_copy)
