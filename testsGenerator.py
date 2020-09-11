import db_manager
from pprint import pprint
import random

import re
import utils

from payloadFormat import get_request_schema, get_response_schema
from payload import get_request_data, get_response_data

db = db_manager.get_db_connection()

MAX_ITER = 20
TESTCASE_COLLECTION = 'testcases'


def get_all_keys(d, res=[], prefix=""):
    for k, v in d.items():
        if isinstance(v, dict):
            res.append(prefix + "-" + k)
            get_all_keys(v, res, prefix + "-" + k)
        else:
            res.append(prefix + "-" + k)
    return res


def map_schemas(s1, s2):    # map s2 -> s1 (response according to request)
    if isinstance(s1, list):
        s1 = s1[0]
    if isinstance(s2, list):
        s2 = s2[0]

    all_matches = []
    for k1, v1 in s1.items():
        match_found = False

        for k2, v2 in s2.items():
            if v1 == v2:
                matched = utils.possibleMatch(k1, k2)
                if matched:
                    match_found = True
                    all_matches.append((k2, k1, 'flat'))

            elif isinstance(v1, list) and v1[0] == v2:
                matched = utils.possibleMatch(k1, k2)
                if matched:
                    match_found = True
                    all_matches.append((k2, k1, 'flat array'))

    return all_matches


def map_request_response_schema(request_schema, response_schema):
    schema_body = request_schema['body']
    schema_path = request_schema['path']
    schema_query = request_schema['query']

    response_body = response_schema['body']

    body_result = []
    path_result = []
    query_result = []

    if schema_body:
        body_result = map_schemas(schema_body, response_body)
    if schema_path:
        path_result = map_schemas(schema_path, response_body)
    if schema_query:
        query_result = map_schemas(schema_query, response_body)

    ret = {
        'body': body_result,
        'path': path_result,
        'query': query_result
    }
    return ret


def map_matched_response(request_data, response_data, matched):
    print("all matched", matched, "\n")
    for match_type in ['body', 'path', 'query']:
        if matched[match_type]:
            for m in matched[match_type]:
                resp_key = m[0]
                req_key = m[1]
                match_level = m[2]

                key_data = None

                if match_level == 'flat':
                    for key in request_data[match_type]:
                        if req_key == key:
                            key_data = request_data[match_type][key]

                    if isinstance(response_data['body'], dict):
                        for key in response_data['body']:   # always body
                            if resp_key == key:
                                if key_data:
                                    response_data['body'][key] = key_data

                    elif isinstance(response_data['body'], list):
                        for r in range(len(response_data['body'])):
                            for key in response_data['body'][r]:
                                if resp_key == key:
                                    if key_data:
                                        response_data['body'][r][key] = key_data

                elif match_level == 'flat array':
                    for key in request_data[match_type]:
                        if req_key == key:
                            key_data = request_data[match_type][key]

                    if isinstance(response_data['body'], dict):
                        for key in response_data['body']:   # always body
                            if resp_key == key:
                                if key_data:
                                    response_data['body'][key] = random.choice(
                                        key_data)

                    elif isinstance(response_data['body'], list):
                        for r in range(len(response_data['body'])):
                            for key in response_data['body'][r]:
                                if resp_key == key:
                                    if key_data:
                                        response_data['body'][r][key] = random.choice(
                                            key_data)

    return response_data


def generate(api_ops_id):
    try:
        testcases = 0

        all_paths = db.paths.find({"api_ops_id": api_ops_id})
        for path in all_paths:
            endpoint = path['path']
            methods = path['allowed_method']

            for m in methods:
                print("===>", endpoint, m)

                request_data = db.requests.find_one(
                    {"api_ops_id": api_ops_id, "path": endpoint, "method": m})
                response_data = db.responses.find_one(
                    {"api_ops_id": api_ops_id, "path": endpoint, "method": m})

                request_schema = get_request_schema(request_data)
                response_schema = get_response_schema(response_data)
                payload_response = get_response_data(response_data)

                testdata = {
                    'api_ops_id': api_ops_id,
                    'endpoint': endpoint,
                    'method': m,
                    'requestSchema': request_schema,
                    'responseSchema': response_schema
                }

                for resp in payload_response:
                    if resp['status'] == '200' or resp['status'] == 'default':
                        resp_body = next(
                            (x for x in response_schema if x['status'] == resp['status']))
                        mapped_result = map_request_response_schema(
                            request_schema, resp_body)

                        ALL_REQUEST_KEYS_SET = set()

                        for iter in range(MAX_ITER):
                            payload_request = get_request_data(request_data)
                            request_keys = get_all_keys(
                                payload_request, res=[], prefix="")
                            request_keys = sorted(request_keys)
                            request_keys = ",".join(request_keys)

                            if request_keys not in ALL_REQUEST_KEYS_SET:
                                ALL_REQUEST_KEYS_SET.add(request_keys)
                                testcases += 1

                                mapped_resp = map_matched_response(
                                    payload_request, resp, mapped_result)

                                testdata['status'] = resp['status']
                                testdata['testcaseId'] = testcases
                                testdata['inputData'] = payload_request
                                testdata['assertionData'] = mapped_resp

                                db_manager.store_document(
                                    TESTCASE_COLLECTION, testdata)

                                # pprint(testdata)
                                # print("\n--------------------------\n")
    except Exception as e:
        print("**Error: " + str(e))


# generate("7e455a71cb514d11a9fcd611138e6f4d")
# generate("6320fb8486164e76823c2d6e7209b2f0")
