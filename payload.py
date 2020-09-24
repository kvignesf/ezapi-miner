# import db_manager
import os
import sys
from pprint import pprint
import random
from random import choice
from string import ascii_lowercase

import requests
import re

from faker import Faker
fake = Faker()


# todo - required elements
STRING_FILTERS = ["minLength", "maxLength", "pattern"]  # todo - pattern
NUMBER_FILTERS = ["minimum", "maximum"]
ARRAY_FILTERS = ["minItems", "maxItems"]
ENUM_FILTERS = ["enum", "default"]

# 32 bit signed integer
MIN_INT = 1  # -1 << 31 Update - Sept 14
MAX_INT = 1 << 31 - 1

# 64 bit signed integer
MIN_LONG = 1  # -1 << 63 Update - Sept 14
MAX_LONG = 1 << 63 - 1

# string
MIN_LEN_STRING = 6
MAX_LEN_STRING = 20

# array
MIN_ITEMS = 1
MAX_ITEMS = 3

# Reference - https://stackoverflow.com/a/26227853


def gen_phone():    # mobile number
    first = str(random.randint(100, 999))
    second = str(random.randint(1, 888)).zfill(3)

    last = (str(random.randint(1, 9998)).zfill(4))
    while last in ['1111', '2222', '3333', '4444', '5555', '6666', '7777', '8888']:
        last = (str(random.randint(1, 9998)).zfill(4))

    return '{}-{}-{}'.format(first, second, last)


EZAPI_VOCAB = {
    "mobileNumber": {
        "value": "gen_phone()",
        "matchType": "word"
    },
    "firstName": {
        "value": "fake.first_name()",
        "matchType": "word"
    },
    "lastName": {
        "value": "fake.last_name()",
        "matchType": "word"
    },
    "city": {
        "value": "fake.city()",
        "matchType": "full"
    },
    "countryCode": {
        "value": "fake.country_code()",
        "matchType": "word"
    },
    "email": {
        "value": "fake.profile()['mail']",
        "matchType": "full"
    },
    "phone": {
        "value": "fake.phone_number()",
        "matchType": "full"
    },
    "country": {
        "value": "fake.country()",
        "matchType": "full"
    },
    "emailAddress": {
        "value": "fake.profile()['mail']",
        "matchType": "word"
    },
    "socialSecurityNumber": {
        "value": "fake.ssn()",
        "matchType": "word"
    },
    "postalCode": {
        "value": "fake.postcode()",
        "matchType": "word"
    },
    "zipCode": {
        "value": "fake.postcode()",
        "matchType": "word"
    },
    "fullName": {
        "value": "fake.name()",
        "matchType": "word"
    },
    "cityName": {
        "value": "fake.city()",
        "matchType": "word"
    },
    "countryName": {
        "value": "fake.country()",
        "matchType": "word"
    },
    "phoneNumber": {
        "value": "fake.phone_number()",
        "matchType": "word"
    },
    "username": {
        "value": "fake.profile()['username']",
        "matchType": "full"
    }
}


def camel_case_words(identifier):
    matches = re.finditer(
        '.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
    res = [m.group(0) for m in matches]
    return res


def isNameMatched(key):
    if not key:
        return False

    res = None
    matched = False

    for k, v in EZAPI_VOCAB.items():
        if v["matchType"] == "full" and key.lower() == k.lower():
            matched = True

        if v["matchType"] == "word":
            words = camel_case_words(k)
            matched = all(w.lower() in key.lower() for w in words)

        if matched:
            try:
                res = eval(v['value'])
            except:
                res = None
            return res

            # A value of type other than (object, array)
            # example - string, integer, time


def generate_random_value(val, key=None):  # key
    val_type = val.get("type")
    val_format = val.get("format")
    val_enum = val.get("enum")
    val_default = val.get("default")
    val_example = val.get("example")
    res = None

    # enum, default
    if val_enum:
        res = random.choice(val_enum)

    elif val_default:
        res = val_default

    # integer - int32, long - int64, float, double
    elif val_format == "int32" or val_type == "integer":
        min_val = val.get("minimum", MIN_INT)
        max_val = val.get("maximum", MAX_INT)

        if val_example and isinstance(val_example, int):
            if val_example > 0:
                min_val = int(val_example * 0.9)
                max_val = int(val_example * 1.1)
            elif val_example < 0:
                min_val = int(val_example * 1.1)
                max_val = int(val_example * 0.9)

        res = random.randint(min_val, max_val)

    elif val_format == "int64" or val_type == "long":
        min_val = val.get("minimum", MIN_LONG)
        max_val = val.get("maximum", MAX_LONG)

        if val_example and isinstance(val_example, int):
            if val_example > 0:
                min_val = int(val_example * 0.9)
                max_val = int(val_example * 1.1)
            elif val_example < 0:
                min_val = int(val_example * 1.1)
                max_val = int(val_example * 0.9)

        res = random.randint(min_val, max_val)

    elif val_type == "number" or (val_format in ["float", "double"]):
        min_val = val.get("minimum", MIN_LONG)
        max_val = val.get("maximum", MAX_LONG)

        if val_example and isinstance(val_example, float):
            if val_example > 0:
                min_val = val_example * 0.9
                max_val = val_example * 1.1
            elif val_example < 0:
                min_val = val_example * 1.1
                max_val = val_example * 0.9

        res = round(random.uniform(min_val, max_val), 2)

    # date - full-date, date-time
    # Reference - https://xml2rfc.tools.ietf.org/public/rfc/html/rfc3339.html#anchor14
    # full-date = date-fullyear "-" date-month "-" date-mday
    # full-time = partial-time time-offset
    # date-time = full-date "T" full-time

    elif val_format == 'date':
        res = fake.date()

    elif val_format == 'date-time':
        res = fake.iso8601()    # todo - tzinfo

    # string
    elif val_type == "string":
        min_len = val.get("minLength", MIN_LEN_STRING)
        max_len = val.get("maxLength", MAX_LEN_STRING)

        res = None
        vocabResult = isNameMatched(key)
        if vocabResult:
            res = vocabResult

        if not res:
            if val_example and isinstance(val_example, str):
                min_len = len(val_example)
                max_len = len(val_example) + 1  # for random.randrange

            random_len = random.randrange(min_len, max_len)
            res = ''.join(choice(ascii_lowercase) for i in range(random_len))

    # boolean
    elif val_type == "boolean":
        res = bool(random.getrandbits(1))

    else:
        # print("***", val_type)
        res = "Unidentified type"

    return res


def generate_random_array(arr, key=None, is_response=False):
    res = []

    # try:
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
                generated = generate_random_object(
                    val, is_response=is_response)
                if generated:
                    res.append(generated)
                # res.append(generate_random_object(val))
            elif val_type == "array" and "items" in val:
                generated = generate_random_array(val, is_response=is_response)
                if generated:
                    res.append(generated)
                # res.append(generate_random_array(val))
            elif val_type not in ["object", "array"]:
                generated = generate_random_value(val, key=key)
                if generated:
                    res.append(generated)
                # res.append(generate_random_value(val))

    # to avoid duplicate entries (example - An array of enum cannot have same enum value twice)
    res = [i for n, i in enumerate(res) if i not in res[:n]]
    # except Exception as e:
    #     exc_type, exc_obj, exc_tb = sys.exc_info()
    #     fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #     print(exc_type, fname, exc_tb.tb_lineno, str(e))

    return res


def generate_random_object(obj, is_response=False):
    res = {}

    # try:
    for key, val in obj["properties"].items():
        val_type = val.get("type")
        val_required = val.get("required", False)
        toss_required = val_required or is_response

        toss = 1  # new - check if field is required or not
        if not toss_required:
            toss = random.randint(0, 1)

        if toss:
            if val_type == "object" and "properties" in val:
                generated = generate_random_object(
                    val, is_response=is_response)
                if generated:
                    res[key] = generated
                # res[key] = generate_random_object(val)
            elif val_type == "array" and "items" in val:
                generated = generate_random_array(
                    val, key=key, is_response=is_response)
                if generated:
                    res[key] = generated
                # res[key] = generate_random_array(val)
            elif val_type not in ["object", "array"]:
                generated = generate_random_value(val, key=key)
                if generated:
                    res[key] = generated
                # res[key] = generate_random_value(val)
    # except Exception as e:
    #     exc_type, exc_obj, exc_tb = sys.exc_info()
    #     fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #     print(exc_type, fname, exc_tb.tb_lineno, str(e))

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

    # try:
    request_body_type = request_body.get("type")

    if request_body_type == "object" and "properties" in request_body:
        payload_body = generate_random_object(
            request_body, is_response=is_response)

    elif request_body_type == "array" and "items" in request_body:
        payload_body = generate_random_array(
            request_body, is_response=is_response)
    # except Exception as e:
    #     print("Body payload error", str(e))

    return payload_body


def get_body_payload_with_missing_required(request_body):
    payload_body = {}
    missed_param_found = False

    request_body_type = request_body.get("type")

    if request_body_type == "object" and "properties" in request_body:
        payload_body = generate_random_object(request_body, True)

        # remove a required param
        required_params = [t1 for t1, t2 in request_body["properties"].items(
        ) if 'required' in t2 and t2['required']]

        if required_params:
            missed_param_found = True
            missed_param = random.choice(required_params)
            payload_body.pop(missed_param, None)

    elif request_body_type == "array" and "items" in request_body:
        payload_body = generate_random_array(
            request_body, is_response=is_response)

        # remove a required param
        array_data = request_body["items"]
        array_data_type = array_data.get("type")

        if array_data_type == 'object' and 'properties' in array_data:
            required_params = [t1 for t1, t2 in array_data["properties"].items(
            ) if 'required' in t2 and t2['required']]

            if required_params:
                missed_param_found = True
                missed_param = random.choice(required_params)

                new_payload_body = payload_body.copy()
                for p in range(len(payload_body)):
                    pdata = payload_body[p]
                    pdata.pop(missed_param, None)
                    new_payload_body.append(pdata)
                payload_body = new_payload_body

    return payload_body, missed_param_found


def get_request_data(request_data, missing_required=False):
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

    missed_found = False

    if missing_required:
        payload_body, missed_found = get_body_payload_with_missing_required(
            request_body)
    else:
        payload_body = get_body_payload(request_body)

    payload_data = {
        "path": payload_path,
        "query": payload_query,
        "form": payload_form,
        "header": payload_header,
        "body": payload_body
    }

    return payload_data, missed_found


def get_response_data(response_data):
    res = []

    response_params = response_data['params']

    for resp_param in response_params:
        status_code = resp_param["status_code"]
        schema = resp_param["schema"]
        headers = resp_param["headers"]

        payload = get_body_payload(
            schema, is_response=True)    # is_response = True
        res.append({'status': status_code, 'body': payload})

    return res

# extract enum data


def extract_directly(request_value):
    enum_data = request_value.get("enum")
    return enum_data


def extract_from_array(request_array, level):
    if level > 1:
        return None

    array_items = request_array["items"]
    array_item_type = array_items.get("type")
    enum_data = None

    if array_item_type and array_item_type not in ("array", "object"):
        enum_data = extract_directly(array_items)

    elif array_item_type == "object" and "properties" in array_items:
        enum_data = extract_from_object(array_items, level + 1)

    return enum_data


def extract_from_object(request_object, level):
    if level > 1:
        return None

    object_data = request_object["properties"]
    enum_result = {}

    for key, val in object_data.items():
        val_type = val.get("type")

        if val_type == "array" and "items" in val:
            enum_data = extract_from_array(val, level + 1)
            if enum_data:
                enum_result[key] = enum_data

        elif val_type and val_type not in ("array", "object"):
            enum_data = extract_directly(val)
            if enum_data:
                enum_result[key] = enum_data

    return enum_result


def extract_from_body(request_body):
    request_body_type = request_body.get("type")
    enum_result = None

    if request_body_type == "array" and "items" in request_body:
        enum_result = extract_from_array(request_body, level=0)

    elif request_body_type == "object" and "properties" in request_body:
        enum_result = extract_from_object(request_body, level=0)

    return enum_result


def extract_from_other_body(request_data):
    enum_result = {}

    if request_data and len(request_data) > 0:
        for req_data in request_data:
            for key, val in req_data.items():
                val_type = val.get("type")

                if val_type == "object" and "properties" in val:
                    res = extract_from_object(val, level=0)
                    if res:
                        enum_result[key] = res

                elif val_type == "array" and "items" in val:
                    res = extract_from_array(val, level=0)
                    if res:
                        enum_result[key] = res

                else:
                    res = extract_directly(val)
                    if res:
                        enum_result[key] = res

    return enum_result


def get_enum_data(request_data):
    request_params = request_data["params"]

    request_path = request_params.get("path")
    request_body = request_params.get("body")
    request_query = request_params.get("query")
    request_form = request_params.get("formData")
    request_header = request_params.get("header")

    enum_result = {
        "path": extract_from_other_body(request_path),
        "query": extract_from_other_body(request_query),
        "form": extract_from_other_body(request_form),
        "header": extract_from_other_body(request_header),
        "body": extract_from_body(request_body) or {}
    }

    return enum_result


def check_enum_covered(payload_request, enum_data, enum_covered):
    entities = ['path', 'query', 'form', 'header', 'body']

    for ent in entities:
        enum_field = enum_data[ent]
        request_field = payload_request[ent]
        covered_enum_field = enum_covered[ent]

        if enum_field:
            for enum_key, enum_val in enum_field.items():
                if enum_key in request_field:
                    # could be an array
                    request_enum_val = request_field[enum_key]
                    covered_enum = covered_enum_field[enum_key]

                    if (not isinstance(request_enum_val, list) and request_enum_val not in covered_enum) or (isinstance(request_enum_val, list) and not set(request_enum_val).issubset(set(covered_enum))):
                        if isinstance(request_enum_val, list):
                            enum_covered[ent][enum_key] = list(
                                set(covered_enum).union(set(request_enum_val)))
                        else:
                            enum_covered[ent][enum_key].append(
                                request_enum_val)
                        return enum_covered, True
                    else:
                        return enum_covered, False

    return enum_covered, False

# res = get_enum_data({
#     "path": "/pet/findByStatus",
#     "method": "get",
#     "params": {
#             "query": [
#                         {
#                             "status": {
#                                 "type": "string",
#                                 "description": "Status values that need to be considered for filter",
#                                 "format": None,
#                                         "required": False,
#                                         "enum": [
#                                             "available",
#                                             "pending",
#                                             "sold"
#                                         ],
#                                 "default": "available"
#                             }
#                         }
#                         ],
#             "header": [],
#             "formData": [],
#         "path": [],
#         "cookie": [],
#         "body": {
#                 "type": "array",
#                 "items": {
#                     "required": [
#                         "name",
#                         "photoUrls"
#                     ],
#                     "type": "object",
#                     "properties": {
#                         "id": {
#                             "type": "integer",
#                             "format": "int64",
#                             "example": 10
#                         },
#                         "name": {
#                             "type": "string",
#                             "example": "doggie"
#                         },
#                         "category": {
#                             "type": "object",
#                             "properties": {
#                                 "id": {
#                                     "type": "integer",
#                                     "format": "int64",
#                                     "example": 1
#                                 },
#                                 "name": {
#                                     "type": "string",
#                                     "example": "Dogs"
#                                 }
#                             },
#                             "xml": {
#                                 "name": "category"
#                             }
#                         },
#                         "photoUrls": {
#                             "type": "array",
#                             "xml": {
#                                 "wrapped": True
#                             },
#                             "items": {
#                                 "type": "string",
#                                 "xml": {
#                                     "name": "photoUrl"
#                                 }
#                             }
#                         },
#                         "tags": {
#                             "type": "array",
#                             "xml": {
#                                 "wrapped": True
#                             },
#                             "items": {
#                                 "type": "object",
#                                 "properties": {
#                                     "id": {
#                                         "type": "integer",
#                                         "format": "int64"
#                                     },
#                                     "name": {
#                                         "type": "string"
#                                     }
#                                 },
#                                 "xml": {
#                                     "name": "tag"
#                                 }
#                             }
#                         },
#                         "status": {
#                             "type": "string",
#                             "description": "pet status in the store",
#                             "enum": [
#                                 "available",
#                                 "pending",
#                                 "sold"
#                             ]
#                         }
#                     },
#                     "xml": {
#                         "name": "pet"
#                     }
#                 }
#             }
#     },
#     "api_ops_id": "49af482258fb42d496df7e6506725589",
#     "filename": "petstore3.json"
# })
