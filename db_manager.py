import pymongo


def get_db_connection(host="localhost", port=27017):
    client = pymongo.MongoClient("localhost", 27017)
    db = client.ezapi
    return db


def store_document(collection, document):
    db = get_db_connection()

    if collection == "apiinfo":
        db_collection = db.apiinfo
    elif collection == "paths":
        db_collection = db.paths
    elif collection == "requests":
        db_collection = db.requests
    elif collection == "responses":
        db_collection = db.responses
    elif collection == "scores":
        db_collection = db.scores
    elif collection == "elements":
        db_collection = db.elements
    elif collection == "testcases":
        db_collection = db.testcases

    db_collection.insert_one(document).inserted_id
