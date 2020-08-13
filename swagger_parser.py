from datetime import datetime
from pprint import pprint
import json
import uuid

import utils
import db_manager

# DB Collections
API_INFO = "apiinfo"
API_PATH = "paths"
API_REQUEST_INFO = "requests"
API_RESPONSE_INFO = "responses"

_HTTP_VERBS = set(["get", "put", "post", "delete", "options", "head", "patch"])
_PARAMETER_TYPES = set(["path", "query", "header", "body", "formData"])


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
                    method_data["description"] = http_method_specs.get("description")
                    method_data["consumes"] = http_method_specs.get("consumes")
                    method_data["produces"] = http_method_specs.get("produces")

                    path_data["method_definition"].append(method_data)

            all_paths.append(path_data)
    return all_paths


def extract_body_param(param):
    param_data = {}

    param_data["name"] = param.get("name")  # fixed-field
    param_data["in"] = param.get("in")  # fixed-field

    # no type field. Check schema (a JSON Schema) for the body structure
    # Reference - https://json-schema.org/learn/getting-started-step-by-step.html
    param_schema = param["schema"]
    param_schema_type = param_schema.get("type")

    if param_schema_type == "array":
        param_data = utils.extract_type_array(
            param_schema, True
        )  # is_json_schema - True

    elif param_schema_type == "object":
        param_data = utils.extract_type_object(
            param_schema, True
        )  # is_json_schema - True

    return param_data


def extract_not_body_param(param):
    param_data = {}
    param_name = param["name"]  # fixed-field

    param_data[param_name] = {}
    param_data[param_name]["type"] = param["type"]  # fixed-filed, if not in body
    param_data[param_name]["description"] = param.get("description")
    param_data[param_name]["format"] = param.get("format")
    param_data[param_name]["required"] = param.get("required", False)

    if param["type"] == "array":
        param_data[param_name]["items"] = utils.extract_type_array(param)
        param_data[param_name]["collectionFormat"] = param.get(
            "collectionFormat", "csv"
        )

    elif param["type"] == "object":
        param_data[param_name]["properties"] = utils.extract_type_object(param)

    return param_data


def extract_response_schema(schema):
    param_data = {}
    param_data["type"] = schema.get("type")

    # todo - handle additionalProperties
    if param_data["type"] == "object" and "properties" in schema:
        param_data["properties"] = utils.extract_type_object(schema)

    elif param_data["type"] == "array":
        param_data["properties"] = utils.extract_type_array(schema)

    return param_data


def get_request_data(jsondata, api_path, method_type):
    all_request_params = {}
    for param in _PARAMETER_TYPES:
        all_request_params[param] = []

    api_params = jsondata["paths"][api_path][method_type].get("parameters")

    for p in api_params:
        pin = p.get("in")  # required, fixed-field

        if pin in _PARAMETER_TYPES:
            param = (
                extract_body_param(p) if pin == "body" else extract_not_body_param(p)
            )
            all_request_params[pin].append(param)

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

        all_response_params.append(param)

    return all_response_params


def get_api_info(jsondata):
    api_info = {}

    KEYS = ["swagger", "info", "host", "basePath", "tags", "schemes"]
    for k in KEYS:
        api_info[k] = jsondata.get(k)

    return api_info


def parse_swagger_api(filepath):
    jsondata = json.loads(open(filepath).read())
    jsondata = utils.deref_json(jsondata)

    api_ops_id = str(uuid.uuid4().hex)

    api_document = get_api_info(jsondata)
    api_document["api_ops_id"] = api_ops_id
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


filepath = "./petstore.json"
parse_swagger_api(filepath)
