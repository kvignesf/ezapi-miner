# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************

import copy
import pymongo


def get_db_connection(dbname, host="localhost", port=27017):
    client = pymongo.MongoClient(host, port)
    db = client[dbname]
    return (client, db)


def store_document(collection, document, db):
    document_copy = copy.deepcopy(document)
    db_collection = db[collection]
    db_collection.insert_one(document_copy)
