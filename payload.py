# import db_manager
from pprint import pprint
import random
from random import choice
from string import ascii_lowercase

import requests

from faker import Faker
fake = Faker()

# db = db_manager.get_db_connection()

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

    # enum, default
    if val_enum:
        res = random.choice(val_enum)

    elif val_default:
        res = val_default

    # integer - int32, long - int64, float, double
    elif val_format == "int32" or val_type == "integer":
        min_val = val.get("minimum", MIN_INT)
        max_val = val.get("maximum", MAX_INT)
        res = random.randint(min_val, max_val)

    elif val_format == "int64" or val_type == "long":
        min_val = val.get("minimum", MIN_LONG)
        max_val = val.get("maximum", MAX_LONG)
        res = random.randint(min_val, max_val)

    elif val_type == "number" or (val_format in ["float", "double"]):
        min_val = val.get("minimum", MIN_LONG)
        max_val = val.get("maximum", MAX_LONG)
        res = random.uniform(min_val, max_val)

    # string
    elif val_type == "string":
        min_len = val.get("minLength", MIN_LEN_STRING)
        max_len = val.get("maxLength", MAX_LEN_STRING)
        random_len = random.randrange(min_len, max_len)
        res = ''.join(choice(ascii_lowercase) for i in range(random_len))

    # boolean
    elif val_type == "boolean":
        res = bool(random.getrandbits(1))

    # date - full-date, date-time
    # Reference - https://xml2rfc.tools.ietf.org/public/rfc/html/rfc3339.html#anchor14
    # full-date = date-fullyear "-" date-month "-" date-mday
    # full-time = partial-time time-offset
    # date-time = full-date "T" full-time

    elif val_type == 'date':
        res = fake.date()

    elif val_type == 'date-time':
        res = fake.iso8601()    # todo - tzinfo

    else:
        # print("***", val_type)
        res = "Unidentified type " + val_type

    return res


def generate_random_array(arr, is_response=False):
    res = []

    par_required = arr.get("required", False)

    val = arr["items"]
    val_type = val.get("type")
    val_required = val.get("required", False)
    toss_required = par_required or is_response

    min_items = val.get("minItems", MIN_ITEMS)
    max_items = val.get("maxItems", MAX_ITEMS)

    if par_required:
        min_items = max(min_items, 1)

    arr_len = random.randint(min_items, max_items)

    toss = 1  # new - check if field is required or not
    if not toss_required:
        toss = random.randint(0, 1)

    if toss:
        for i in range(arr_len):
            if val_type == "object" and "properties" in val:    # todo - additionalProperties
                generated = generate_random_object(val, is_response)
                if generated:
                    res.append(generated)
                # res.append(generate_random_object(val))
            elif val_type == "array" and "items" in val:
                generated = generate_random_array(val, is_response)
                if generated:
                    res.append(generated)
                # res.append(generate_random_array(val))
            elif val_type not in ["object", "array"]:
                generated = generate_random_value(val)
                if generated:
                    res.append(generated)
                # res.append(generate_random_value(val))

    # to avoid duplicate entries (example - An array of enum cannot have same enum value twice)
    res = [i for n, i in enumerate(res) if i not in res[:n]]

    return res


def generate_random_object(obj, is_response=False):
    res = {}

    for key, val in obj["properties"].items():
        val_type = val.get("type")
        val_required = val.get("required", False)
        toss_required = val_required or is_response

        toss = 1  # new - check if field is required or not
        if not toss_required:
            toss = random.randint(0, 1)

        if toss:
            if val_type == "object" and "properties" in val:
                generated = generate_random_object(val, is_response)
                if generated:
                    res[key] = generated
                # res[key] = generate_random_object(val)
            elif val_type == "array" and "items" in val:
                generated = generate_random_array(val, is_response)
                if generated:
                    res[key] = generated
                # res[key] = generate_random_array(val)
            elif val_type not in ["object", "array"]:
                generated = generate_random_value(val)
                if generated:
                    res[key] = generated
                # res[key] = generate_random_value(val)

    return res


# path, query, formData, headers
def get_other_payload(request_data):
    payload_data = {}

    if request_data and len(request_data) > 0:
        for req_data in request_data:

            for key, val in req_data.items():
                # payload_data[key]=None
                val_type = val.get("type")
                val_required = val.get("required", False)

                toss = 1  # new - check if field is required or not
                if not val_required:
                    toss = random.randint(0, 1)

                if toss:
                    if val_type == "object" and "properties" in val:
                        generated = generate_random_object(val)
                        if generated:
                            payload_data[key] = generated
                        # payload_data[key] = generate_random_object(val)
                    elif val["type"] == "array" and "items" in val:
                        generated = generate_random_array(val)
                        if generated:
                            payload_data[key] = generated
                        # payload_data[key] = generate_random_array(val)
                    elif val_type not in ["object", "array"]:
                        generated = generate_random_value(val)
                        if generated:
                            payload_data[key] = generated
                        # payload_data[key]=generate_random_value(val)

    return payload_data


def get_body_payload(request_body, is_response=False):
    payload_body = {}

    request_body_type = request_body.get("type")

    if request_body_type == "object" and "properties" in request_body:
        payload_body = generate_random_object(request_body, is_response)

    elif request_body_type == "array" and "items" in request_body:
        payload_body = generate_random_array(request_body, is_response)

    return payload_body


def get_request(base_url, endpoint, m):
    url = base_url + endpoint
    r = None

    if m == 'get':
        r = requests.get(url)
    if m == 'post':
        r = requests.post(url)
    if m == 'put':
        r = requests.put(url)
    if m == 'delete':
        r = requests.delete(url)
    if m == 'patch':
        r = requests.patch(url)
    if m == 'option':
        r = requests.option(url)
    if m == 'head':
        r = requests.head(url)

    return r


def get_request_data(request_data):
    # request_data=db.requests.find_one(
    #     {"api_ops_id": api_ops_id, "path": path, "method": method})

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


def get_response_data(response_data):
    # response_data = db.responses.find_one(
    #     {"api_ops_id": api_ops_id, "path": path, "method": method})

    res = []

    response_params = response_data['params']

    for resp_param in response_params:
        status_code = resp_param["status_code"]
        schema = resp_param["schema"]
        headers = resp_param["headers"]

        payload = get_body_payload(schema, True)    # is_response = True
        res.append({'status': status_code, 'body': payload})

    return res


# api_ops_id = "f57c06e9075544958eb776f27d0b8208"
# all_paths = list(db.paths.find({"api_ops_id": api_ops_id}))
# for path in all_paths:
#     endpoint = path['path']
#     methods = path['allowed_method']

#     for m in methods:
#         print("\n-------------------------\n")
#         print(endpoint, m, "==>")
#         payload_request = get_request_data(api_ops_id, endpoint, m)
#         print("---------- Request payload ==> \n")
#         pprint(payload_request)
#         print("\n\n")
#         payload_response = get_response_data(api_ops_id, endpoint, m)
#         print("---------- Response payload ==> \n")
#         pprint(payload_response)
#         print("\n\n")
