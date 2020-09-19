import db_manager
from pprint import pprint
import random

import copy

import re
import utils
from urllib.parse import urlencode

import sys
import os

from payloadFormat import get_request_schema, get_response_schema
from payload import get_request_data, get_response_data, check_enum_covered, get_enum_data

MAX_ITER = 20
TESTCASE_COLLECTION = 'testcases'
VIRTUAL_SERVICE_COLLECTION = 'virtual'
_HTTP_VERBS = set(["get", "put", "post", "delete", "options", "head", "patch"])
_HTTP_COMMON_VERBS = set(["get", "put", "post", "delete", "patch"])


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


def get_virtual_collection_data(testdata):
    try:
        virtual_service_data = {
            'httpMethod': testdata['method'],
            'headers': testdata['inputData']['header'],
            'formData': testdata['inputData']['form'],
            'requestBody': testdata['inputData']['body'],
            'responseStatusCode': testdata['status'],
            'responseBody': testdata.get('assertionData', {})
        }

        virtual_service_data['responseBody'] = testdata.get(
            'assertionData', {})
        virtual_service_data['responseBody'] = virtual_service_data['responseBody'].get(
            'body', {})

        endpoint = testdata['endpoint']
        pathData = testdata['inputData']['path']
        queryData = testdata['inputData']['query']

        print(endpoint, end=' , ')

        endpoint = endpoint.format(**pathData)
        tmp = urlencode(queryData)
        if tmp:
            endpoint += '?' + tmp
        print(endpoint)
        virtual_service_data['endpoint'] = endpoint

        return virtual_service_data
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno, str(e))


def generate(api_ops_id):
    try:
        testcases = 0

        client, db = db_manager.get_db_connection()
        all_paths = db.paths.find({"api_ops_id": api_ops_id})
        for path in all_paths:
            filename = path['filename']
            endpoint = path['path']
            methods = path['allowed_method']

            for m in methods:
                method_operation_id = next(x['operationId']
                                           for x in path['method_definition'] if x['method'] == m) or 'OperationId'
                # print("===>", endpoint, m)

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
                    'responseSchema': response_schema,
                    'filename': filename,
                    'test_case_type': 'F',
                    'test_case_name': None,
                    'operation_id': method_operation_id,
                    'request_response_mapping': None,
                    'testcaseId': None,
                    'delete': False
                }

                for resp in payload_response:
                    if resp['status'] == '200' or resp['status'] == 'default':
                        testdata['test_case_name'] = method_operation_id + '__P'
                        resp_body = next(
                            (x for x in response_schema if x['status'] == resp['status']))
                        mapped_result = map_request_response_schema(
                            request_schema, resp_body)
                        testdata['request_response_mapping'] = mapped_result

                        enum_data = get_enum_data(request_data)

                        ALL_REQUEST_KEYS_SET = set()
                        ALL_ENUM_COVERED = {}

                        entities = ['path', 'query', 'form', 'header', 'body']
                        ALL_ENUM_COVERED = copy.deepcopy(enum_data)

                        for ent in entities:
                            for key, val in ALL_ENUM_COVERED[ent].items():
                                ALL_ENUM_COVERED[ent][key] = []

                        for iter in range(MAX_ITER):
                            payload_request, _ = get_request_data(request_data)
                            request_keys = get_all_keys(
                                payload_request, res=[], prefix="")
                            request_keys = sorted(request_keys)
                            request_keys = ",".join(request_keys)

                            ALL_ENUM_COVERED, enum_check_res = check_enum_covered(
                                payload_request, enum_data, ALL_ENUM_COVERED)

                            if request_keys not in ALL_REQUEST_KEYS_SET or enum_check_res:
                                ALL_REQUEST_KEYS_SET.add(request_keys)
                                testcases += 1

                                mapped_resp = map_matched_response(
                                    payload_request, resp, mapped_result)

                                testdata['status'] = resp['status']
                                testdata['testcaseId'] = testcases
                                testdata['inputData'] = payload_request
                                testdata['assertionData'] = mapped_resp
                                testdata['description'] = 'ok'

                                # pprint(testdata)
                                # print("\n----------------------------\n")

                                virtual_testdata = get_virtual_collection_data(
                                    testdata)

                                db_manager.store_document(
                                    TESTCASE_COLLECTION, testdata)
                                db_manager.store_document(
                                    VIRTUAL_SERVICE_COLLECTION, virtual_testdata)

                        testdata['request_response_mapping'] = None

                    # missing mandatory parameter, Deceptive request (add %/ in endpoint uri)
                    elif resp['status'] == '400':
                        testdata['test_case_name'] = method_operation_id + '__P'
                        ALL_REQUEST_KEYS_SET = set()

                        for iter in range(MAX_ITER):
                            payload_request, missed_found = get_request_data(
                                request_data, missing_required=True)
                            if missed_found:
                                request_keys = get_all_keys(
                                    payload_request, res=[], prefix="")
                                request_keys = sorted(request_keys)
                                request_keys = ",".join(request_keys)

                                if request_keys not in ALL_REQUEST_KEYS_SET:
                                    ALL_REQUEST_KEYS_SET.add(request_keys)
                                    testcases += 1

                                    testdata['status'] = resp['status']
                                    testdata['testcaseId'] = testcases
                                    testdata['inputData'] = payload_request
                                    testdata['description'] = 'missing mandatory parameter'

                                    # pprint(testdata)
                                    # print("\n----------------------------\n")

                                    virtual_testdata = get_virtual_collection_data(
                                        testdata)

                                    db_manager.store_document(
                                        TESTCASE_COLLECTION, testdata)
                                    db_manager.store_document(
                                        VIRTUAL_SERVICE_COLLECTION, virtual_testdata)

                        # Deceptive request
                        testcases += 1
                        payload_request, _ = get_request_data(request_data)
                        testdata['status'] = resp['status']
                        testdata['testcaseId'] = testcases
                        testdata['inputData'] = payload_request

                        tmp = testdata['endpoint'].split('{')
                        tmp[0] += '%/'
                        if len(tmp) > 1:
                            tmp[0] += '{'
                        testdata['endpoint'] = ''.join(tmp)
                        testdata['description'] = 'deceptive request'

                        # pprint(testdata)
                        # print("\n----------------------------\n")

                        virtual_testdata = get_virtual_collection_data(
                            testdata)

                        db_manager.store_document(
                            TESTCASE_COLLECTION, testdata)
                        db_manager.store_document(
                            VIRTUAL_SERVICE_COLLECTION, virtual_testdata)

                        testdata['endpoint'] = endpoint

                    # not found (chnage endpoint uri)
                    elif resp['status'] == '404':
                        testdata['test_case_name'] = method_operation_id + '__P'
                        testcases += 1
                        payload_request, _ = get_request_data(request_data)
                        testdata['status'] = resp['status']
                        testdata['testcaseId'] = testcases
                        testdata['inputData'] = payload_request

                        tmp = testdata['endpoint'].split('?')
                        tmp[0] += 'abc'
                        if len(tmp) > 1:
                            tmp[0] += '?'
                        testdata['endpoint'] = ''.join(tmp)
                        testdata['description'] = 'uri not found'

                        # pprint(testdata)
                        # print("\n----------------------------\n")

                        virtual_testdata = get_virtual_collection_data(
                            testdata)

                        db_manager.store_document(
                            TESTCASE_COLLECTION, testdata)
                        db_manager.store_document(
                            VIRTUAL_SERVICE_COLLECTION, virtual_testdata)

                        testdata['endpoint'] = endpoint

                    # method not allowed
                    elif resp['status'] == '405':
                        testdata['test_case_name'] = method_operation_id + '__P'
                        payload_request, _ = get_request_data(request_data)
                        for cm in _HTTP_COMMON_VERBS:
                            if cm not in methods:
                                testcases += 1
                                testdata['method'] = cm
                                testdata['status'] = resp['status']
                                testdata['testcaseId'] = testcases
                                testdata['inputData'] = payload_request
                                testdata['description'] = 'method not allowed'

                                # pprint(testdata)
                                # print("\n----------------------------\n")

                                virtual_testdata = get_virtual_collection_data(
                                    testdata)

                                db_manager.store_document(
                                    TESTCASE_COLLECTION, testdata)
                                db_manager.store_document(
                                    VIRTUAL_SERVICE_COLLECTION, virtual_testdata)

        res = {
            'success': True,
            'message': 'ok',
            'status': 200
        }
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno, str(e))
        res = {
            'success': False,
            'errorType': type(e).__name__,
            'error': str(e),
            'message': 'Some error has occured in generating testcases',
            'status': 500,
        }

    return res


# generate("49af482258fb42d496df7e6506725589")    # petstore3
# generate("f5554781ed934013afa2858d8909aed6")    # petstore
