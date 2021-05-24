import copy
import pymongo


def get_db_connection(dbname="apidesign", host="localhost", port=27017):
    client = pymongo.MongoClient(host, port)
    # client = pymongo.MongoClient(
    #     "mongodb://root:JRVvuh9D5V0IZxCW@34.66.45.162:27017/?authSource=admin",
    #     Connect=False,
    # )
    db = client[dbname]
    return (client, db)


def store_document(collection, document, db):
    document_copy = copy.deepcopy(document)
    db_collection = db[collection]
    db_collection.insert_one(document_copy)
