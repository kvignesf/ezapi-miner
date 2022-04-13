# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************


import os, sys

from api_designer import mongo
from api_designer.utils.common import *
from api_designer.utils.schema_manager import crawl_schema

from pprint import pprint

_HTTP_VERBS = set(["get", "put", "post", "delete", "options", "head", "patch"])
_PARAMETER_TYPES = set(["path", "query", "header", "body", "formData", "cookie"])
OTHER_FIELDS = set(
    [
        "enum",
        "default",
        "pattern",
        "example",
        "minimum",
        "maximum",
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
        "allowEmptyValue",
    ]
)


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
                        key: http_method_specs[key]
                        for key in http_method_specs.keys()
                        & {"tags", "summary", "description", "operationId"}
                    }

                    if "operationId" not in method_data:
                        method_data["operationId"] = http_method + " " + path

                    curr_path = merge_dict(curr_path, method_data)  # common.py
                    all_paths.append(curr_path)

    return all_paths


# (query, path, header) - param type can't be an object
def extract_param_data(param, components):
    if "ezapi_ref" in param:
        param_ref = param["ezapi_ref"]
        param_ref = param_ref.split("/")[2:]  # example - #/definitions/User

        param = components
        for pr in param_ref:
            param = param[pr]

    param_data = {}
    param_name = param["name"]

    param_data[param_name] = {}
    for f in ["type", "description", "format"]:
        param_data[param_name][f] = param.get(f)
    param_data[param_name]["required"] = param.get("required", False)

    for f in OTHER_FIELDS:
        if f in param:
            param_data[param_name][f] = param.get(f)

    # csv - foo, bar (other formats - ssv, tsv, pipes)
    # multi - only valid for "query" or "formData"
    if param.get("type") == "array":  # items required
        param_data[param_name]["items"] = param.get("items")
        param_data[param_name]["collectionFormat"] = param.get(
            "collectionFormat", "csv"
        )

    return param_data


def extract_request_body(param, components):
    if "ezapi_ref" in param:
        param_ref = param["ezapi_ref"]
        param_ref = param_ref.split("/")[2:]  # example - #/definitions/User

        param = components
        for pr in param_ref:
            param = param[pr]

    param_data = {}
    # todo - other fields, user name is different than body
    param_name = param.get("name")
    param_desc = param.get("description")
    param_req = param.get("required", False)
    param_schema = param["schema"]  # required

    param_data = param["schema"]
    return param_data


def extract_request_data(path_data, path, method, components):
    all_request_params = {}
    for p in _PARAMETER_TYPES:
        all_request_params[p] = []

    # parameters applicable for all the operations under this path
    common_parameters = path_data[path][method].get("parameters", [])
    parameters = path_data[path].get("parameters", [])

    parameters += common_parameters

    for param in parameters:
        param_in = param["in"]

        if param_in in ("query", "header", "path"):
            param_data = extract_param_data(param, components)
            all_request_params[param_in].append(param_data)
        elif param_in == "body":
            all_request_params["body"] = extract_request_body(param, components)

    return all_request_params


def extract_response_data(path_data, path, method, components):
    responnses = []
    response_data = path_data[path][method].get("responses")

    for resp, resp_spec in response_data.items():
        # descriptio is required
        obj = {
            "status_code": resp,
            "description": resp_spec["description"],
            "content": resp_spec.get("schema"),
            "headers": resp_spec.get("headers"),
        }
        responnses.append(obj)
    return responnses


def parse_swagger(jsondata, projectid, spec_filename, db):
    print("Inside parsing swagger 3.0")

    fullspec_collection = "raw_spec"
    fullspec_document = {
        "filename": spec_filename,
        "projectid": projectid,
        "data": jsondata,
    }
    mongo.store_document(fullspec_collection, fullspec_document, db)

    # definitions -> componennts -> schemas
    jsonstr = json.dumps(jsondata)
    jsonstr = jsonstr.replace("#/definitions/", "#/components/schemas/")
    jsondata = json.loads(jsonstr)

    path_data = jsondata.get("paths")
    components_data_2 = jsondata.get("definitions")
    components_data = {"schemas": components_data_2}

    all_paths = extract_path_data(path_data)
    all_parameters = {}

    for path in all_paths:
        endpoint = path["endpoint"]
        method = path["method"]

        request_data = extract_request_data(
            path_data, endpoint, method, components_data
        )
        response_data = extract_response_data(
            path_data, endpoint, method, components_data
        )

        path["requestData"] = request_data
        path["responseData"] = response_data

        path_collection = "paths"
        path_document = {
            "filename": spec_filename,
            "projectid": projectid,
            "data": path,
        }
        mongo.store_document(path_collection, path_document, db)

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

    try:
        pass
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno, str(e))

    return res
