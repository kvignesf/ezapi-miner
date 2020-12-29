# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************

from apiops import config
from apiops import utils

from pprint import pprint
import operator

API_PARAM_FUNCTIONS = 'scores'
API_ELEMENTS_FUNCTIONS = 'elements'


def get_apiops_description(all_paths, path, method):
    for p in all_paths:
        if path == p['path']:
            all_methods = p["method_definition"]
            for m in all_methods:
                if method == m['method']:
                    #return (m.get("summary"), m.get("apiops_description"))
                    return (m.get("summary"), m.get("operationId"))
    return (None, None)


def cluster_paths(api_ops_id, db):  # path -> tags mapping
    all_paths = db.paths.find({"api_ops_id": api_ops_id})
    paths_tag = {}

    for p in all_paths:
        path = p['path']
        # assuming all methods belongs to same set of tags

        try:
            tags = p['method_definition'][0].get('tags')
        except:
            tags = None

        if tags and path not in paths_tag:
            paths_tag[path] = tags
        else:
            paths_tag[path] = ["API MODEL"]

    return paths_tag


def extract_request_params(api_ops_id, paths_tag, tags, db):
    result = {}

    for t in tags:
        # score 1 for each occurence and an additional 0.5 for required fields
        result[t] = {
            # 'paths': [],  # removed from the collection
            'request_fields': {},
            'required_fields': {},
            'response_fields': {},
            'fields_score': {}
        }

    all_requests = db.requests.find({"api_ops_id": api_ops_id})
    all_responses = db.responses.find({"api_ops_id": api_ops_id})

    for r in all_requests:
        path = r['path']
        params = r['params']

        body_data = [params['body']]
        headers_data = params['header']
        path_data = params['path']
        form_data = params['formData']
        query_data = params['query']

        all_data = body_data + headers_data + path_data + form_data + query_data

        tags = paths_tag.get(path, [])

        for t in tags:

            if t not in result:
                result[t] = {
                    'request_fields': {},
                    'required_fields': {},
                    'response_fields': {},
                    'fields_score': {}
                }
            # result[t]['paths'].append(path)   # removed from the collection

            for d in all_data:
                if "type" in d and d["type"] == "object":
                    d = d["properties"]
                if "type" in d and d["type"] == "array":
                    d = d["items"]

                if "type" not in d:  # Only flat keys. Don't count nested
                    for k, v in d.items():
                        if k not in result[t]['request_fields']:
                            result[t]['request_fields'][k] = 0
                            result[t]['fields_score'][k] = 0

                        result[t]['request_fields'][k] += 1
                        result[t]['fields_score'][k] += 1

                        if 'required' in v and v['required']:
                            if k not in result[t]['required_fields']:
                                result[t]['required_fields'][k] = 0
                            result[t]['required_fields'][k] += 1
                            result[t]['fields_score'][k] += 0.5

    for r in all_responses:
        path = r['path']
        tags = paths_tag.get(path, [])

        for t in tags:
            params = r['params']
            for p in params:
                status = p['status_code']
                if status.isdigit() and int(int(status)/100) == 2:   # 2xx status only
                    schema = p.get('schema')

                    if 'type' in schema and schema['type'] == 'object' and "properties" in schema:
                        schema = schema['properties']
                    elif 'type' in schema and schema['type'] == 'array' and "items" in schema:
                        schema_items = schema['items']
                        if 'type' in schema_items and schema_items['type'] == 'object' and "properties" in schema:
                            schema = schema_items['properties']

                    if schema and 'type' not in schema:  # Only flat keys. Don't count nested
                        for k, v in schema.items():
                            if k not in result[t]['response_fields']:
                                result[t]['response_fields'][k] = 0
                            result[t]['response_fields'][k] += 1

    return result


def map_request_elements(api_ops_id, paths_tag, tags, db):
    result = {}
    for t in tags:
        result[t] = {}  # element, description mapping

    all_requests = db.requests.find({"api_ops_id": api_ops_id})
    all_paths = list(db.paths.find({"api_ops_id": api_ops_id}))

    for r in all_requests:
        path = r['path']
        method = r['method']
        params = r['params']

        body_data = [params['body']]
        headers_data = params['header']
        path_data = params['path']
        form_data = params['formData']
        query_data = params['query']
        all_data = body_data + headers_data + path_data + form_data + query_data

        tags = paths_tag.get(path, [])
        for t in tags:  # can be multiple tags for a path
            if t not in result:
                result[t] = {}

            for d in all_data:

                if "type" in d and d["type"] == "object":
                    d = d["properties"]
                if "type" in d and d["type"] == "array":
                    d = d["items"]
                if "type" not in d:  # Only flat keys. Don't count nested
                    for k, v in d.items():

                        if k not in result[t]:
                            result[t][k] = []

                        path_summary, path_description = get_apiops_description(
                            all_paths, path, method)
                        result[t][k].append({
                            'path': path,
                            'method': method,
                            'summary': path_summary,
                            'description': path_description})

    return result


def score_param_elements(api_ops_id, db):
    try:
        tags_info = utils.get_all_tags(api_ops_id, db)
        tags_paths = utils.get_tags_from_paths(api_ops_id, db)
        tags = list(set(tags_info + tags_paths))

        # mapping from path -> tags (multiple tags are allowed)
        paths_tag = cluster_paths(api_ops_id, db)

        # tag is the keyword
        result_params = extract_request_params(
            api_ops_id, paths_tag, tags, db)
        result_elements = map_request_elements(
            api_ops_id, paths_tag, tags, db)

        # tag name contains special characters, which maybe not proper mongo document key format. So change it accordingly
        result_params_new = {'api_ops_id': api_ops_id, 'data': []}
        result_elements_new = {'api_ops_id': api_ops_id, 'data': []}

        for key, val in result_params.items():
            tmp = {'tag': key, 'data': val}
            result_params_new['data'].append(tmp)

        for key, val in result_elements.items():
            tmp = {'tag': key, 'data': val}
            result_elements_new['data'].append(tmp)

        config.store_document(
            API_PARAM_FUNCTIONS, result_params_new, db)
        config.store_document(
            API_ELEMENTS_FUNCTIONS, result_elements_new, db)

        res = {
            'status': 200,
            'message': 'ok',
            'success': True
        }
    except Exception as e:
        res = {
            'status': 500,
            'errorType': type(e).__name__,
            'error': str(e),
            'message': 'Some error has occured in extracting data',
            'success': False
        }

    return res
