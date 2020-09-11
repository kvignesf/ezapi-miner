# import db_manager
from pprint import pprint
import random
from random import choice
from string import ascii_lowercase

import requests

from faker import Faker
fake = Faker()

# client, db = db_manager.get_db_connection()

# toss = random.randint(0, 1)

# todo - required elements
STRING_FILTERS = ["minLength", "maxLength", "pattern"]  # todo - pattern
NUMBER_FILTERS = ["minimum", "maximum"]
ARRAY_FILTERS = ["minItems", "maxItems"]
ENUM_FILTERS = ["enum", "default"]

# 32 bit signed integer
MIN_INT = -1 << 31
MAX_INT = 1 << 31 - 1

# 64 bit signed integer
MIN_LONG = -1 << 63
MAX_LONG = 1 << 63 - 1

# string
MIN_LEN_STRING = 6
MAX_LEN_STRING = 20

# array
MIN_ITEMS = 1
MAX_ITEMS = 3


# A value of type other than (object, array)
# example - string, integer, time
def generate_random_value(val):
    val_type = val.get("type")
    val_format = val.get("format")
    val_enum = val.get("enum")
    val_default = val.get("default")

    if val_format:
        return val_format
    return val_type


def generate_random_array(arr):
    res = []

    val = arr["items"]
    val_type = val.get("type")

    if val_type == "object" and "properties" in val:    # todo - additionalProperties
        res.append(generate_random_object(val))
    elif val_type == "array" and "items" in val:
        res.append(generate_random_array(val))
    elif val_type not in ["object", "array"]:
        res.append(generate_random_value(val))

    # to avoid duplicate entries (example - An array of enum cannot have same enum value twice)
    res = [i for n, i in enumerate(res) if i not in res[:n]]

    return res


def generate_random_object(obj):
    res = {}

    for key, val in obj["properties"].items():
        val_type = val.get("type")

        if val_type == "object" and "properties" in val:
            res[key] = generate_random_object(val)
        elif val_type == "array" and "items" in val:
            res[key] = generate_random_array(val)
        elif val_type not in ["object", "array"]:
            res[key] = generate_random_value(val)

    return res


# path, query, formData, headers
def get_other_payload(request_data):
    payload_data = {}

    if request_data and len(request_data) > 0:
        for req_data in request_data:

            for key, val in req_data.items():
                # payload_data[key]=None
                val_type = val.get("type")

                if val_type == "object" and "properties" in val:
                    payload_data[key] = generate_random_object(val)
                elif val["type"] == "array" and "items" in val:
                    payload_data[key] = generate_random_array(val)
                elif val_type not in ["object", "array"]:
                    payload_data[key] = generate_random_value(val)

    return payload_data


def get_body_payload(request_body):
    payload_body = {}

    request_body_type = request_body.get("type")

    if request_body_type == "object" and "properties" in request_body:
        payload_body = generate_random_object(request_body)

    elif request_body_type == "array" and "items" in request_body:
        payload_body = generate_random_array(request_body)

    return payload_body


def get_request_schema(request_data):
    request_params = request_data["params"]

    request_path = request_params.get("path")
    request_body = request_params.get("body")
    request_query = request_params.get("query")
    request_form = request_params.get("formData")
    request_header = request_params.get("header")

    payload_path = {}
    payload_query = {}
    payload_form = {}
    payload_header = {}
    payload_body = {}

    payload_path = get_other_payload(request_path)
    payload_query = get_other_payload(request_query)
    payload_form = get_other_payload(request_form)
    payload_header = get_other_payload(request_header)
    payload_body = get_body_payload(request_body)

    payload_data = {
        "path": payload_path,
        "query": payload_query,
        "form": payload_form,
        "header": payload_header,
        "body": payload_body
    }

    return payload_data


def get_response_schema(response_data):
    res = []

    response_params = response_data['params']

    for resp_param in response_params:
        status_code = resp_param["status_code"]
        schema = resp_param["schema"]
        headers = resp_param["headers"]

        payload = get_body_payload(schema)
        res.append({'status': status_code, 'body': payload})

    return res
