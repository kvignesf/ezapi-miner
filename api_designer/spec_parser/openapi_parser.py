# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************


import os, sys

from requests.api import request

from api_designer.utils.schema_manager import crawl_schema
from api_designer import mongo
from api_designer.utils.common import *
import os, sys

_HTTP_VERBS = set(["get", "put", "post", "delete", "options", "head", "patch"])
_PARAMETER_TYPES = set(
    ["path", "query", "header", "body", "formData", "cookie"]
)  # cookie - openapi 3.0

_SCHEMA_FIELDS = [
    "enum",
    "default",
    "pattern",
    "example",
    "maximum",
    "minimum",
    "minLength",
    "maxLength",
    "minItems",
    "maxItems",
    "allowEmptyValue",
]
_PARAMETER_TYPES = set(["path", "query", "header", "cookie", "body", "formData"])


def extract_path_data(path_data):
    all_paths = []

    for path, path_spec in path_data.items():
        if path[0] == "/":
            for http_method, http_method_specs in path_spec.items():
                if http_method in _HTTP_VERBS:
                    curr_path = {}
                    curr_path["endpoint"] = path
                    curr_path["method"] = http_method

                    method_data = {
                        key: http_method_specs.get(key)
                        for key in http_method_specs.keys()
                        & {"tags", "summary", "description", "operationId"}
                    }

                    if "operationId" not in method_data:
                        method_data["operationId"] = http_method + " " + path

                    curr_path = merge_dict(curr_path, method_data)  # common.py
                    all_paths.append(curr_path)
    return all_paths


def extract_param_data(param, components):
    if "ezapi_ref" in param:
        param_ref = param["ezapi_ref"]

        # Ignore ('#', 'components') prefix
        # example - #/components/parameters/aflac-processing-style
        param_ref = param_ref.split("/")[2:]

        param = components
        for pr in param_ref:
            param = param[pr]

    param_data = {}
    param_name = param["name"]  # required field
    # required field (query, header, path, or cookie)
    param_in = param["in"]

    param_data[param_name] = {}
    param_data[param_name]["description"] = param.get("description")
    param_data[param_name]["required"] = param.get("required")

    param_schema = param["schema"]
    param_data[param_name]["type"] = param_schema.get("type")
    param_data[param_name]["format"] = param_schema.get("format")

    for f in _SCHEMA_FIELDS:
        if f in param_schema:
            param_data[param_name][f] = param_schema.get(f)

    if "explode" in param:
        param_data[param_name]["explode"] = param["explode"]  # query - form

    # In case of explode = True
    # Reference - petstore /findByTags /findByStatus
    if (
        "type" in param_schema
        and param_schema["type"] == "array"
        and "items" in param_schema
    ):
        param_data[param_name]["items"] = param_schema.get("items")

    # elif "type" in param_schema and param_schema["type"] == "object":
    #     pass  # todo

    return param_in, param_data


def extract_body_content(body_content):
    # check for 'type' or 'ezapi_ref' for dereferencing
    return body_content["schema"]


def extract_form_content(form_content):
    return form_content["schema"]


def extract_request_body(request_body, components):
    if "ezapi_ref" in request_body:  # body refers to requestBodies
        body_ref = request_body["ezapi_ref"]
        body_ref = body_ref.split("/")[2:]

        request_body = components
        for br in body_ref:
            request_body = request_body[br]

    content = request_body["content"]

    body_content = None
    form_content = None

    if "application/json" in content:
        body_content = content["application/json"]
    elif "application/x-www-form-urlencoded" in content:
        body_content = content["application/x-www-form-urlencoded"]
    elif "application/octet-stream" in content:
        form_content = content["application/octet-stream"]

    if body_content:
        body_content = extract_body_content(body_content)
    elif form_content:
        form_content = extract_form_content(form_content)

    body_content = body_content or []
    form_content = form_content or []

    return body_content, form_content


def extract_request_data(path_data, path, method, components):
    all_request_params = {}
    for p in _PARAMETER_TYPES:
        all_request_params[p] = []

    parameters = path_data[path][method].get("parameters", [])
    request_body = path_data[path][method].get("requestBody")

    common_parameters = path_data[path].get("parameters", [])
    parameters += common_parameters

    # A parameter data can be in path, query, header, formData, or cookie
    for param in parameters:
        param_in, param_data = extract_param_data(param, components)

        if param_in in _PARAMETER_TYPES:
            all_request_params[param_in].append(param_data)

    if request_body:
        (
            all_request_params["body"],
            all_request_params["formData"],
        ) = extract_request_body(
            request_body, components
        )  # may contains ezapi_ref

    return all_request_params


def extract_response_data(path_data, path, method, components):
    # check for 'type' or 'ezapi_ref' for dereferencing if not None
    responses = []
    response_data = path_data[path][method].get("responses")

    for resp, resp_spec in response_data.items():  # http_status and response object
        if "ezapi_ref" in resp_spec:
            resp_ref = resp_spec["ezapi_ref"]
            resp_ref = resp_ref.split("/")[2:]

            resp_spec = components
            for rr in resp_ref:
                resp_spec = resp_spec[rr]

        obj = {
            "status_code": resp,
            "description": resp_spec.get("description"),
            "content": resp_spec.get("content"),
            "headers": resp_spec.get("headers"),
            "links": resp_spec.get("links"),
        }

        if obj["content"]:
            obj["content"] = obj["content"].get("application-json")

            if obj["content"]:
                obj["content"] = obj["content"].get("schema")

        responses.append(obj)
    return responses


def parse_openapi(jsondata, projectid, spec_filename, db):
    try:
        fullspec_collection = "raw_spec"
        fullspec_document = {
            "filename": spec_filename,
            "projectid": projectid,
            "data": jsondata,
        }
        mongo.store_document(fullspec_collection, fullspec_document, db)

        path_data = jsondata.get("paths")
        components_data = jsondata.get("components")

        all_paths = extract_path_data(path_data)
        all_parameters = {}

        # Extract request, response data for individual path (endpoint) wise
        for path in all_paths:
            endpoint = path["endpoint"]
            method = path["method"]

            request_data = extract_request_data(
                path_data, endpoint, method, components_data
            )
            response_data = extract_response_data(
                path_data, endpoint, method, components_data
            )

            # Extract all parameters
            for param in request_data["path"] + request_data["query"]:
                for k, v in param.items():
                    if k not in all_parameters:
                        all_parameters[k] = v
                        all_parameters[k]["header"] = False

            for param in request_data["header"]:
                for k, v in param.items():
                    if k not in all_parameters:
                        all_parameters[k] = v
                        all_parameters[k]["header"] = True

            path["requestData"] = request_data
            path["responseData"] = response_data

            path_collection = "paths"
            path_document = {
                "filename": spec_filename,
                "projectid": projectid,
                "data": path,
            }
            mongo.store_document(path_collection, path_document, db)

        parameter_collection = "parameters"
        parameter_document = {
            "filename": spec_filename,
            "projectid": projectid,
            "data": all_parameters,
        }
        mongo.store_document(parameter_collection, parameter_document, db)

        component_collection = "components"
        component_document = {
            "filename": spec_filename,
            "projectid": projectid,
            "data": components_data,
        }
        mongo.store_document(component_collection, component_document, db)

        schemas = components_data["schemas"]
        crawled_schemas = crawl_schema(schemas)

        for cs in crawled_schemas:
            schema_collection = "schemas"
            schema_document = {
                "filename": spec_filename,
                "projectid": projectid,
                "data": cs,
            }
            mongo.store_document(schema_collection, schema_document, db)

        res = {"success": True, "status": 200, "message": "ok"}

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno, str(e))

        res = {
            "success": False,
            "errorType": type(e).__name__,
            "error": str(e),
            "message": "Some error has occured in parsing data",
            "status": 500,
        }

    return res
