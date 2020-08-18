import pymongo
import db_manager

from pprint import pprint
import operator

db = db_manager.get_db_connection()
api_ops_id = 'a66eb33de61d4a82bb8e0c728a56f5cb'

API_PARAM_FUNCTIONS = 'param_functions'


def get_all_tags(api_ops_id):
    apiinfo = db.apiinfo.find_one({"api_ops_id": api_ops_id})
    return apiinfo["tags"]


def cluster_paths(api_ops_id):  # path -> tags mapping
    all_paths = db.paths.find({"api_ops_id": api_ops_id})
    paths_tag = {}

    for p in all_paths:
        path = p['path']
        # assuming all methods belongs to same set of tags
        tags = p['method_definition'][0].get('tags')

        if tags and path not in paths_tag:
            paths_tag[path] = tags

    return paths_tag


def extract_request_params(api_ops_id, paths_tag, tags):
    result = {}

    for tag in tags:
        t = tag['name']
        # score 1 for each occurence and an additional 0.5 for required fields
        result[t] = {
            'paths': [],
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

        body_data = params['body']
        headers_data = params['header']
        path_data = params['path']
        form_data = params['formData']
        query_data = params['query']

        all_data = body_data + headers_data + path_data + form_data + query_data

        tags = paths_tag[path]

        for t in tags:
            result[t]['paths'].append(path)

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
        tags = paths_tag[path]

        for t in tags:
            params = r['params']
            for p in params:
                status = p['status_code']
                if status.isdigit() and int(int(status)/100) == 2:   # 2xx status only
                    schema = p.get('schema')

                    if 'type' in schema and schema['type'] == 'object' and "properties" in schema:
                        pprint(schema)
                        print("\n")
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


def param_functions(api_ops_id):
    tags = get_all_tags(api_ops_id)
    paths_tag = cluster_paths(api_ops_id)
    result = extract_request_params(api_ops_id, paths_tag, tags)

    pprint(result)

    result['api_ops_id'] = api_ops_id
    db_manager.store_document(API_PARAM_FUNCTIONS, result)


param_functions(api_ops_id)
