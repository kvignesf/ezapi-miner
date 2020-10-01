from swagger_parser import parse_swagger_api
from param_functions import handle_param_functions
from visualizer import fetch_sankey_data
from testsGenerator import generate

from dumpMongo import dumpData


def run_apiops_model(filepath, filename, dbname, api_ops_id):
    parsed_result = parse_swagger_api(filepath, filename, dbname, api_ops_id)
    if 'success' in parsed_result and parsed_result['success']:
        # api_ops_id = parsed_result['data']['api_ops_id']

        extraction_result = handle_param_functions(api_ops_id, dbname)
        if 'success' in extraction_result and extraction_result['success']:

            visualizer_result = fetch_sankey_data(api_ops_id, dbname)
            if 'success' in visualizer_result and visualizer_result['success']:

                testcase_result = generate(api_ops_id, dbname)
                if 'success' in testcase_result and testcase_result['success']:

                    # print("dumping collection")
                    # dumpData(dbname)

                    ret = {
                        'success': True,
                        'status': 200,
                        'message': 'ok',
                        'stage': 'tests',
                        'data': {
                            'api_ops_id': api_ops_id,
                            'testcases_count': testcase_result['data']['testcases_count']
                        }
                    }
                    print(ret)
                    return ret

                else:
                    testcase_result['stage'] = 'sankey'
                    print(testcase_result)
                    return testcase_result
            else:
                visualizer_result['stage'] = 'scored'
                print(visualizer_result)
                return visualizer_result
        else:
            extraction_result['stage'] = 'parsed'
            print(extraction_result)
            return extraction_result
    else:
        parsed_result['stage'] = 'not scored'
        print(parsed_result)
        return parsed_result


def parse_swagger_openapi(filepath, filename):
    parsed_result = parse_swagger_api(filepath, filename, dbname="ezapi")
    if 'success' in parsed_result and parsed_result['success']:
        api_ops_id = parsed_result['data']['api_ops_id']

        extraction_result = handle_param_functions(api_ops_id, dbname="ezapi")
        if 'success' in extraction_result and extraction_result['success']:
            return parsed_result
        else:
            return extraction_result
    else:
        return parsed_result


def get_sankey_data(api_ops_id):
    sankey_result = fetch_sankey_data(api_ops_id, dbname="ezapi")
    return sankey_result


def generate_test_cases(api_ops_id):
    res = generate(api_ops_id, dbname="ezapi")
    return res
