import json
from pprint import pprint
import random
import string

import requests
import pymongo

import db_manager


def generate_random_str():
    return "".join(
        random.SystemRandom().choice(string.ascii_uppercase + string.digits)
        for _ in range(20)
    )


def generate_random_int():
    return random.randrange(1, 10000, 1)


def generate_random_array(arr):
    result = []

    val = arr["items"]

    if val["type"] == "object":
        result.append(generate_random_object(val))

    elif val["type"] == "array":
        result.append(generate_random_array(val))

    else:
        result.append(generate_other(val))

    return result


def generate_random_object(obj):
    result = {}

    for key, val in obj["properties"].items():

        if val["type"] == "object":
            result[key] = generate_random_object(val)

        elif val["type"] == "array":
            result[key] = generate_random_array(val)

        else:
            result[key] = generate_other(val)

    return result


def generate_other(val):
    result = None

    if val["type"] == "string":
        result = generate_random_str()
    elif val["type"] == "integer":
        result = generate_random_int()

    return result


def get_api_info(api_ops_id, path, method):
    db = db_manager.get_db_connection()
    apiinfo = db.apiinfo.find_one({"api_ops_id": api_ops_id})

    host = apiinfo["host"]
    basePath = apiinfo["basePath"]
    schemes = apiinfo["schemes"]

    baseurl = "https://" + host + basePath
    request_url = baseurl + path

    request_data = db.requests.find_one(
        {"api_ops_id": api_ops_id, "path": path, "method": method}
    )

    request_param = request_data["params"]

    data = {}
    headers = {}

    request_body = request_param["body"]
    request_header = request_param["header"]

    for rb in request_body:
        if "type" not in rb:  # dictionary
            for key, val in rb.items():

                data[key] = None

                if val["type"] == "object":
                    tmp = generate_random_object(val)
                    data[key] = tmp

                elif val["type"] == "array":
                    tmp = generate_random_array(val)
                    data[key] = tmp

                else:
                    tmp = generate_other(val)
                    data[key] = tmp

    print(data)
    print(type(data))

    jsondata = json.dumps(data)
    headers = {"Content-type": "application/json", "Accept": "application/json"}

    print(request_url)
    r = requests.post(request_url, data=jsondata, headers=headers)
    print(r.status_code)
    print(r.json())


get_api_info("b587c0cc7a1d41aebee648ace8a2a7a3", "/pet", "post")
