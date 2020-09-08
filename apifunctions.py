from swagger_parser import parse_swagger_api
from param_functions import handle_param_functions
from visualizer import fetch_sankey_data


def parse_swagger_openapi(filepath, filename):
    parsed_result = parse_swagger_api(filepath, filename)
    if 'success' in parsed_result and parsed_result['success']:
        api_ops_id = parsed_result['data']['api_ops_id']

        extraction_result = handle_param_functions(api_ops_id)
        if 'success' in extraction_result and extraction_result['success']:
            return parsed_result
        else:
            return extraction_result
    else:
        return parsed_result


def get_sankey_data(api_ops_id):
    sankey_result = fetch_sankey_data(api_ops_id)
    return sankey_result
