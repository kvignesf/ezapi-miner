from pprint import pprint

from apifunctions import parse_swagger_openapi, generate_test_cases

import json
import codecs

import os
dir = '/Users/shbham/Desktop/workspace/turing/ezapi/testdata'
paths = os.listdir(dir)

for path in paths:
    if "json" in path:
        filepath = dir + "/" + path
        print("\n------------------------------\n")
        print(filepath)

        parsed_result = parse_swagger_openapi(filepath, "testfile")
        if 'success' in parsed_result and parsed_result['success']:
            api_ops_id = parsed_result['data']['api_ops_id']

            testcases_result = generate_test_cases(api_ops_id)
            if 'success' in testcases_result and testcases_result['success']:
                print("** Correct")
            else:
                print("** Error Generation")
        else:
            print("** Error Parsing")
