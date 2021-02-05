# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************

from pprint import pprint
import random
import copy

import re
from urllib.parse import urlencode

import sys
import os

from apiops import config
from apiops import utils

from apiops.tester.payloadFormat import get_request_schema, get_response_schema
from apiops.tester.payload import get_request_data, get_response_data, check_enum_covered, get_enum_data

MAX_ITER = 20

TESTCASE_COLLECTION = 'testcases'
VIRTUAL_SERVICE_COLLECTION = 'virtual'
TESTRESULT_COLLECTION = 'test_result'

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

                    if isinstance(response_data, dict):
                        for key in response_data:   # always body
                            if resp_key == key:
                                if key_data:
                                    response_data[key] = key_data

                    elif isinstance(response_data, list):
                        for r in range(len(response_data)):
                            for key in response_data[r]:
                                if resp_key == key:
                                    if key_data:
                                        response_data[r][key] = key_data

                elif match_level == 'flat array':
                    for key in request_data[match_type]:
                        if req_key == key:
                            key_data = request_data[match_type][key]

                    if isinstance(response_data, dict):
                        for key in response_data:   # always body
                            if resp_key == key:
                                if key_data:
                                    response_data[key] = random.choice(
                                        key_data)

                    elif isinstance(response_data, list):
                        for r in range(len(response_data)):
                            for key in response_data[r]:
                                if resp_key == key:
                                    if key_data:
                                        response_data[r][key] = random.choice(
                                            key_data)

    return response_data

def getCountByKey(inputpayload):
    tc_suffix = ""
    elmntsNmbr = 0

    for k, v in inputpayload.items():
        if len(v) > 0:
            #if isinstance(v,dict):
            #    v = getCountByKey(v)
            #print("k...", k)
            #print("v...", v, len(v))
            nmbrelmnts = len(v)
            #print("..type", type(v), v.items())
            for a, b in v.items():
                if isinstance(b, dict):
                    #print("a...", a)
                    #print("b...", b, len(b))
                    nmbrelmnts = len(b)

            tc_suffix = tc_suffix + k[0]+ str(nmbrelmnts)
            elmntsNmbr = elmntsNmbr + nmbrelmnts
            #tc_suffix = k[0]+str(nmbrelmnts) if tc_suffix is "" else tc_suffix + "|" + k[0]+str(nmbrelmnts)
    #print("..tc_suffix", tc_suffix)
    print ("...elmntsNmbr", elmntsNmbr)
    return elmntsNmbr

def gettotalElements(fullSuffString):
    totalCnt = 0
    for idx in range(len(fullSuffString)):
        if not idx % 2:
            pass
        else:
            totalCnt = int(totalCnt) + int(fullSuffString[idx])
    return totalCnt


def sumOftotalElements(fullSuffString):
    totalCnt = 0
    #fullSuffString.split("|").
    for idx in range(len(fullSuffString)):
        if not idx % 2:
            pass
        else:
            totalCnt = int(totalCnt) + int(fullSuffString[idx])
    return totalCnt

def getInputAsParams(inputpayload):
    param_suffix = ""
    elem_suffix= ""
    for k, v in inputpayload.items():
        if len(v) > 0:
            #if isinstance(v,dict):
            #    v = getCountByKey(v)
            print("k...", k)
            print("v...", v, len(v))
            #nmbrelmnts = len(v)
            #print("..type", type(v), v.items())
            for a, b in v.items():
                if isinstance(b, dict):
                    print("a...", a)
                    print("b...", b, len(b))
                    nmbrelmnts = len(b)
                    param_suffix = param_suffix + "," + k + "("+str(len(b))+")"
                else:
                    print(".paramin", k+"("+a+")")
                    elem_suffix = a if elem_suffix is "" else elem_suffix + "," + a
            param_suffix = k + "("+elem_suffix+")" if param_suffix is "" else param_suffix + "," + k + "("+elem_suffix+")"


        else:
            print("param", k+"(0)")
            param_suffix = k+"(0)" if param_suffix is "" else param_suffix + "," + k+"(0)"


            #tc_suffix += k[0]+str(nmbrelmnts)
    print("..param_suffix", param_suffix)
    return param_suffix

def get_virtual_collection_data(testdata):
    try:
        virtual_service_data = {
            'api_ops_id': testdata['api_ops_id'],
            'httpMethod': testdata['method'],
            'headers': testdata['inputData']['header'],
            'formData': testdata['inputData']['form'],
            'requestBody': testdata['inputData']['body'],
            'responseStatusCode': testdata['status']
        }

        virtual_service_data['responseBody'] = testdata.get(
            'assertionData', {})

        endpoint = testdata['endpoint']
        pathData = testdata['inputData']['path']
        queryData = testdata['inputData']['query']

        endpoint = endpoint.format(**pathData)
        tmp = urlencode(queryData)
        if tmp:
            endpoint += '?' + tmp
        virtual_service_data['endpoint'] = endpoint

        return virtual_service_data
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno, str(e))


def process_test_generator(api_ops_id, db):
    try:
        random.seed(42)
        testcases = 0

        testcase_result = {}

        all_paths = db.paths.find({"api_ops_id": api_ops_id})
        for path in all_paths:
            filename = path['filename']
            endpoint = path['path']
            methods = path['allowed_method']
            method_definition = path['method_definition']

            method_tags = {t['method']: t['tags'] for t in method_definition}

            testcase_result['api_ops_id'] = api_ops_id
            testcase_result['filename'] = filename
            testcase_result['run1'] = {}
            testcase_result['run2'] = {}
            testcase_result['run3'] = {}

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
                    'delete': False,
                    'resource': "resource"+m if method_tags[m] is None else method_tags[m]
                }

                for resp in payload_response:
                    if resp['status'] == '200' or resp['status'] == 'default':
                        testdata['test_case_name'] = method_operation_id + '__P'
                        if isinstance(testdata["resource"],list):
                            resourceVal = testdata["resource"][0]
                        else:
                            resourceVal = testdata["resource"]
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
                                    payload_request, resp['body'], mapped_result)

                                testdata['status'] = resp['status']
                                testdata['testcaseId'] = testcases
                                testdata['inputData'] = payload_request
                                testdata['assertionData'] = mapped_resp
                                testdata['description'] = 'ok'
                                suffix = getCountByKey(payload_request)
                                testdata['parameters'] = getInputAsParams(payload_request)
                                print("..suffix..", suffix)
                                # cntDataSets = gettotalElements(suffix)
                                # print("cntDataSets",cntDataSets)
                                testcasenameSuff = " datasets" if suffix > 1 else " dataset"
                                # testdata['test_case_name'] = "Validate_" + m + "_" + method_operation_id + "_" + resp['status'] + "_API with " + suffix
                                testdata['test_case_name'] = "Validate " + resp['status'] + " response for " + method_operation_id + " of " + filename.split('.')[0] + " API using " + str(suffix) + testcasenameSuff

                                # pprint(testdata)
                                # print("\n----------------------------\n")

                                virtual_testdata = get_virtual_collection_data(
                                    testdata)

                                config.store_document(
                                    TESTCASE_COLLECTION, testdata, db)
                                config.store_document(
                                    VIRTUAL_SERVICE_COLLECTION, virtual_testdata, db)

                        testdata['request_response_mapping'] = None

                    # missing mandatory parameter, Deceptive request (add %/ in endpoint uri)
                    elif resp['status'] == '400':
                        #testdata['test_case_name'] = method_operation_id + '__P'
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
                                    testdata['assertionData'] = resp['body']
                                    suffix = getCountByKey(payload_request)
                                    #testdata['test_case_name'] = "Validate_" + m + "_" + method_operation_id + "_" + resp['status'] + "_API with " + str(suffix)
                                    testdata['test_case_name'] = "Validate " + resp['status'] + " response for " + method_operation_id + " of " + filename.split('.')[0] + " API using " + str(suffix) + " datasets"

                                    # pprint(testdata)
                                    # print("\n----------------------------\n")

                                    virtual_testdata = get_virtual_collection_data(
                                        testdata)

                                    config.store_document(
                                        TESTCASE_COLLECTION, testdata, db)
                                    config.store_document(
                                        VIRTUAL_SERVICE_COLLECTION, virtual_testdata, db)

                        # Deceptive request
                        testcases += 1
                        payload_request, _ = get_request_data(request_data)
                        testdata['status'] = resp['status']
                        testdata['testcaseId'] = testcases
                        testdata['inputData'] = payload_request
                        testdata['assertionData'] = resp['body']

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

                        config.store_document(
                            TESTCASE_COLLECTION, testdata, db)
                        config.store_document(
                            VIRTUAL_SERVICE_COLLECTION, virtual_testdata, db)

                        testdata['endpoint'] = endpoint

                    # not found (chnage endpoint uri)
                    elif resp['status'] == '404':
                        testdata['test_case_name'] = method_operation_id + '__P'
                        testcases += 1
                        payload_request, _ = get_request_data(request_data)
                        testdata['status'] = resp['status']
                        testdata['testcaseId'] = testcases
                        testdata['inputData'] = payload_request
                        testdata['assertionData'] = resp['body']
                        suffix = getCountByKey(payload_request)
                        #testdata['test_case_name'] = "Validate_" + m + "_" + method_operation_id + "_" + resp['status'] + "_API with " + str(suffix)
                        testdata['test_case_name'] = "Validate " + resp['status'] + " response for " + method_operation_id + " of " + filename.split('.')[0] + " API using " + str(suffix) + " datasets"

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

                        config.store_document(
                            TESTCASE_COLLECTION, testdata, db)
                        config.store_document(
                            VIRTUAL_SERVICE_COLLECTION, virtual_testdata, db)

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
                                testdata['assertionData'] = resp['body']
                                suffix = getCountByKey(payload_request)
                                #testdata['test_case_name'] = "Validate_" + m + "_" + method_operation_id + "_" + resp['status'] + "_API with " + str(suffix)
                                testdata['test_case_name'] = "Validate " + resp['status'] + " response for " + method_operation_id + " of " + filename.split('.')[0] + " API using " + str(suffix) + " datasets"

                                # pprint(testdata)
                                # print("\n----------------------------\n")

                                virtual_testdata = get_virtual_collection_data(
                                    testdata)

                                config.store_document(
                                    TESTCASE_COLLECTION, testdata, db)
                                config.store_document(
                                    VIRTUAL_SERVICE_COLLECTION, virtual_testdata, db)

        config.store_document(
            TESTRESULT_COLLECTION, testcase_result, db)

        res = {
            'success': True,
            'message': 'ok',
            'status': 200,
            'data': {
                'testcases_count': testcases
            }
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
