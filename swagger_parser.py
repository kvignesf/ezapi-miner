import codecs
from datetime import datetime
from pprint import pprint
import json
import os
import sys
import uuid

import jsonref

import utils
import db_manager

# DB Collections
API_INFO = "apiinfo"
API_PATH = "paths"
API_REQUEST_INFO = "requests"
API_RESPONSE_INFO = "responses"

_HTTP_VERBS = set(["get", "put", "post", "delete", "options", "head", "patch"])
_PARAMETER_TYPES = set(["path", "query", "header", "body",
                        "formData", "cookie"])   # cookie - openapi 3.0

# minimum, maximum, minLength, maxLength, minItems, maxItems ...
OTHER_FIELDS = set(["enum", "default"])


def get_all_paths(jsondata):
    paths = jsondata["paths"]
    all_paths = []

    for path, path_spec in paths.items():
        if path[0] == "/":
            path_data = {}
            path_data["path"] = path
            path_data["allowed_method"] = []
            path_data["method_definition"] = []

            for http_method, http_method_specs in path_spec.items():
                if http_method in _HTTP_VERBS:
                    path_data["allowed_method"].append(http_method)

                    method_data = {}
                    method_data["method"] = http_method
                    method_data["tags"] = http_method_specs.get("tags")
                    method_data["summary"] = http_method_specs.get("summary")
                    method_data["description"] = http_method_specs.get(
                        "description")
                    method_data["operationId"] = http_method_specs.get(
                        "operationId")
                    method_data["consumes"] = http_method_specs.get("consumes")
                    method_data["produces"] = http_method_specs.get("produces")

                    method_data["apiops_description"] = utils.extract_apiops_description(
                        method_data["operationId"], method_data["description"], method_data["summary"])

                    path_data["method_definition"].append(method_data)

            all_paths.append(path_data)
    return all_paths


def extract_body_param(param):
    param_data = {}

    param_name = param.get("name")
    param_required = param.get("required", False)
    param_description = param.get("description")

    param_schema = param["schema"]
    param_schema_type = param_schema.get("type")

    if not param_schema_type:
        if "items" in param_schema:
            param_schema_type["type"] = "array"
            param_schema_type = "array"
        elif "properties" in param_schema:
            param_schema_type["type"] = "object"
            param_schema_type = "object"

    if param_name and param_name != "body":
        param_data["type"] = "object"
        param_data["required"] = param_required
        param_data["description"] = param_description
        param_data["properties"] = {}
        param_data["properties"][param_name] = {}

        param_data["properties"][param_name]["type"] = param_schema_type

        if param_schema_type == "object" and "properties" in param_schema:
            param_data["properties"][param_name]["properties"] = utils.extract_type_object(
                param_schema, True
            )  # is_json_schema - True

        if param_schema_type == "array" and "items" in param_schema:
            param_data["properties"][param_name]["items"] = utils.extract_type_array(
                param_schema, True
            )  # is_json_schema - True
    else:
        param_data["type"] = param_schema_type

        if param_schema_type == "object" and "properties" in param_schema:
            param_data["properties"] = utils.extract_type_object(
                param_schema, True
            )  # is_json_schema - True

        if param_schema_type == "array" and "items" in param_schema:
            param_data["items"] = utils.extract_type_array(
                param_schema, True
            )  # is_json_schema - True

    return param_data


# def extract_body_param(param):
#     param_data = {}

#     """
#     param_data["name"] = param.get("name")  # fixed-field
#     param_data["in"] = param.get("in")  # fixed-field
#     """

#     # no type field. Check schema (a JSON Schema) for the body structure
#     # Reference - https://json-schema.org/learn/getting-started-step-by-step.html
#     param_name = param.get("name")
#     param_required = param.get("required", False)
#     param_description = param.get("description")

#     param_schema = param["schema"]
#     param_schema_type = param_schema.get("type")
#     param_schema_format = param_schema.get("format")

#     if param_name and param_name != "body":
#         param_data["type"] = "object"
#         param_data["properties"] = {}
#         param_data["properties"][param_name] = {}

#     if param_schema_type == None:
#         if "items" in param_schema:
#             param_schema_type["type"] = "array"
#             param_schema_type = "array"
#         elif "properties" in param_schema:
#             param_schema_type["type"] = "object"
#             param_schema_type = "object"

#     param_data["type"] = param_schema_type
#     param_data["required"] = param.get("required", False)

#     if param_schema_type == "object" and "properties" in param_schema:
#         param_data["properties"] = utils.extract_type_object(
#             param_schema, True
#         )  # is_json_schema - True

#     else:
#         param_data["type"] = "object"
#         param_data["required"] = True   # body - always required
#         param_data["properties"] = {}

#         if param_name:
#             param_data["properties"][param_name] = {}
#             param_data["properties"][param_name]["type"] = param_schema_type
#             param_data["properties"][param_name]["format"] = param_schema_format
#             param_data["properties"][param_name]["required"] = param_required
#             param_data["properties"][param_name]["description"] = param_description

#         if param_schema_type == "array" and "items" in param_schema:
#             print("\narray schema")
#             pprint(param_schema)
#             param_data["properties"][param_name]["items"] = utils.extract_type_array(
#                 param_schema, True)  # is_json_schema - True

#     print("\n\n -> param data\n")
#     pprint(param_data)
#     return param_data


# def extract_body_param(param):
#     param_data = {}

#     """
#     param_data["name"] = param.get("name")  # fixed-field
#     param_data["in"] = param.get("in")  # fixed-field
#     """

#     # no type field. Check schema (a JSON Schema) for the body structure
#     # Reference - https://json-schema.org/learn/getting-started-step-by-step.html
#     param_schema = param["schema"]
#     param_schema_type = param_schema.get("type")

#     if param_schema_type == None:
#         if "items" in param_schema:
#             param_schema_type["type"] = "array"
#             param_schema_type = "array"
#         elif "properties" in param_schema:
#             param_schema_type["type"] = "object"
#             param_schema_type = "object"

#     param_data["type"] = param_schema_type
#     param_data["required"] = param.get("required", False)

#     if param_schema_type == "array" and "items" in param_schema:
#         param_data["items"] = utils.extract_type_array(
#             param_schema, True
#         )  # is_json_schema - True

#     elif param_schema_type == "object" and "properties" in param_schema:
#         param_data["properties"] = utils.extract_type_object(
#             param_schema, True
#         )  # is_json_schema - True

#     return param_data


def extract_not_body_param(param):
    param_data = {}
    param_name = param["name"]  # fixed-field

    param_data[param_name] = {}
    # fixed-filed, if not in body
    param_data[param_name]["type"] = param.get("type")
    param_data[param_name]["description"] = param.get("description")
    param_data[param_name]["format"] = param.get("format")
    param_data[param_name]["required"] = param.get("required", False)

    if "type" in param and param["type"] == "array":
        param_data[param_name]["items"] = utils.extract_type_array(param)
        param_data[param_name]["collectionFormat"] = param.get(
            "collectionFormat", "csv"
        )

    elif "type" in param and param["type"] == "object":
        param_data[param_name]["properties"] = utils.extract_type_object(param)

    for f in OTHER_FIELDS:
        if f in param:
            param_data[param_name][f] = param.get(f)

    # openapi 3.0
    if "schema" in param:
        param_schema = param["schema"]
        param_data[param_name]["type"] = param_schema.get(
            "type")
        param_data[param_name]["format"] = param_schema.get(
            "format")

        for f in OTHER_FIELDS:
            if f in param_schema:
                param_data[param_name][f] = param_schema.get(f)

        if "type" in param_schema and param_schema["type"] == "array":
            param_data[param_name]["items"] = utils.extract_type_array(
                param_schema)
            param_data[param_name]["collectionFormat"] = param_schema.get(
                "collectionFormat", "csv"
            )

        elif "type" in param_schema and param_schema["type"] == "object":
            param_data[param_name]["properties"] = utils.extract_type_object(
                param_schema)

    return param_data


def extract_response_schema(schema):
    param_data = {}
    schema_type = schema.get("type")

    if schema_type == None:
        if "items" in schema:
            schema["type"] = "array"
            schema_type = "array"
        elif "properties" in schema:
            schema["type"] = "object"
            schema_type = "object"

    param_data["type"] = schema_type

    # todo - handle additionalProperties
    if schema_type == "object" and "properties" in schema:
        param_data["properties"] = utils.extract_type_object(schema)

    elif schema_type == "array" and "items" in schema:
        param_data["items"] = utils.extract_type_array(schema)

    return param_data


def get_request_data(jsondata, api_path, method_type):
    print("------------------>", api_path, method_type, "\n")
    all_request_params = {}
    for param in _PARAMETER_TYPES:
        all_request_params[param] = []

    all_request_params["body"] = {}  # body cannot be an array

    api_params = jsondata["paths"][api_path][method_type].get("parameters", [])

    # Parameters that are applicable for all the operations described under this path
    # Reference - https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#pathsObject
    if not api_params:
        api_params = jsondata["paths"][api_path].get("parameters", [])

    for p in api_params:
        pin = p.get("in")  # required, fixed-field

        if pin in _PARAMETER_TYPES:
            if pin == "body":
                param = extract_body_param(p)
                all_request_params[pin] = param
            else:
                param = extract_not_body_param(p)
                all_request_params[pin].append(param)

    # openapi 3.0
    requestBody = jsondata["paths"][api_path][method_type].get("requestBody")
    if requestBody:
        body_content = requestBody["content"]

        if "application/json" in body_content:
            body_content = body_content["application/json"]
        elif "application/x-www-form-urlencoded" in body_content:
            body_content = body_content["application/x-www-form-urlencoded"]
        elif "application/octet-stream" in body_content:
            body_content = body_content["application/octet-stream"]

        param = extract_body_param(body_content)
        all_request_params["body"] = param

    return all_request_params


def get_response_data(jsondata, api_path, method_type):
    all_response_params = []

    api_params = jsondata["paths"][api_path][method_type].get("responses")

    for resp, resp_specs in api_params.items():
        param = {
            "status_code": resp,
            "description": resp_specs["description"],
            "schema": {},
            "headers": {},
        }

        if "schema" in resp_specs:
            param["schema"] = extract_response_schema(resp_specs["schema"])

        # openapi 3.0
        if "content" in resp_specs:
            resp_schema = resp_specs["content"].get("application/json")
            param["schema"] = extract_response_schema(resp_schema["schema"])

        all_response_params.append(param)

    return all_response_params


def get_api_info(jsondata):
    api_info = {}

    KEYS = ["swagger", "info", "host", "basePath",
            "tags", "schemes", "openapi", "servers"]
    for k in KEYS:
        api_info[k] = jsondata.get(k)

    return api_info


def parse_swagger_api(filepath, filename):
    try:
        jsondata = json.load(codecs.open(filepath, 'r', 'utf-8-sig'))
        # jsondata = json.loads(open(filepath).read().decode('utf-8-sig'))
        # jsondata = json.loads(open(filepath).read())
        jsondata = utils.deref_json(jsondata)

        api_ops_id = str(uuid.uuid4().hex)
        print("api_ops_id generated ", api_ops_id)

        api_document = get_api_info(jsondata)
        api_document["api_ops_id"] = api_ops_id
        api_document["filename"] = filename
        db_manager.store_document(API_INFO, api_document)

        all_paths = get_all_paths(jsondata)

        for path in all_paths:
            path["api_ops_id"] = api_ops_id
            db_manager.store_document(API_PATH, path)

            p = path["path"]
            methods = path["allowed_method"]

            for m in methods:
                request_data = get_request_data(jsondata, p, m)

                response_data = get_response_data(jsondata, p, m)

                request_document = {
                    "path": p,
                    "method": m,
                    "params": request_data,
                    "api_ops_id": api_ops_id,
                }

                response_document = {
                    "path": p,
                    "method": m,
                    "params": response_data,
                    "api_ops_id": api_ops_id,
                }

                db_manager.store_document(API_REQUEST_INFO, request_document)
                db_manager.store_document(API_RESPONSE_INFO, response_document)

        tags_info = utils.get_all_tags(api_ops_id)
        tags_paths = utils.get_tags_from_paths(api_ops_id)
        tags = list(set(tags_info + tags_paths))

        res = {
            'success': True,
            'message': 'ok',
            'status': 200,
            'data': {
                'api_ops_id': api_ops_id,
                'tags': tags
            }
        }
    except jsonref.JsonRefError as e:
        res = {
            'success': False,
            'errorType': type(e).__name__,
            'message': "JSON Referencing Error. APIOPS doesn't support remote reference",
            'error': str(e),
            'status': 500,
        }
    except json.JSONDecodeError as e:
        res = {
            'success': False,
            'errorType': type(e).__name__,
            'message': "Please validate your JSON",
            'error': str(e),
            'status': 500,
        }
    except Exception as e:
        res = {
            'success': False,
            'errorType': type(e).__name__,
            'error': str(e),
            'message': 'Some error has occured in parsing data',
            'status': 500,
        }
    return res
