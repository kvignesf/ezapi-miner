from swagger_parser import parse_swagger_api
from param_functions import handle_param_functions
from visualizer import fetch_sankey_data
from testsGenerator import generate


def run_apiops_model(filepath, filename, dbname):
    parsed_result = parse_swagger_api(filepath, filename, dbname)
    if 'success' in parsed_result and parsed_result['success']:
        api_ops_id = parsed_result['data']['api_ops_id']

        extraction_result = handle_param_functions(api_ops_id, dbname)
        if 'success' in extraction_result and extraction_result['success']:

            visualizer_result = fetch_sankey_data(api_ops_id, dbname)
            if 'success' in visualizer_result and visualizer_result['success']:

                testcase_result = generate(api_ops_id, dbname)
                if 'success' in testcase_result and testcase_result['success']:

                    ret = {
                        'success': True,
                        'status': 200,
                        'message': 'ok',
                        'data': {
                            'api_ops_id': api_ops_id,
                            'testcases_count': testcase_result['data']['testcases_count']
                        }
                    }
                    return ret

                else:
                    return testcase_result
            else:
                return visualizer_result
        else:
            return extraction_result
    else:
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
