# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************


import os, sys
from numpy import empty

from api_designer import mongo
from api_designer.utils.common import *
from api_designer.utils.schema_manager import crawl_schema
import shortuuid, uuid

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
    reso_path_array = []

    for path, path_spec in path_data.items():
        if path[0] == "/":
            path_obj = {}
            path_name = path
            path_obj["pathId"] = shortuuid.uuid()
            path_obj["pathName"] = path_name
            oper_array = []
            for http_method, http_method_specs in path_spec.items():
                if http_method in _HTTP_VERBS:
                    oper_obj = {}
                    common_id = str(uuid.uuid4())
                    curr_path = {}
                    curr_path["endpoint"] = path
                    curr_path["method"] = http_method
                    curr_path["id"] = common_id

                    method_data = {
                        key: http_method_specs[key]
                        for key in http_method_specs.keys()
                        & {"tags", "summary", "description", "operationId"}
                    }

                    if "operationId" not in method_data:
                        method_data["operationId"] = http_method + " " + path

                    curr_path = merge_dict(curr_path, method_data)  # common.py
                    oper_obj["operationType"] = http_method
                    oper_obj["operationId"] = common_id
                    oper_obj["operationName"] = method_data["operationId"]
                    oper_obj["Description"] = method_data.get("description", "missingDescription")
                    oper_array.append(oper_obj)
                    all_paths.append(curr_path)
            path_obj["operations"] = oper_array
            reso_path_array.append(path_obj)

    return all_paths, reso_path_array


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
    param_data[param_name]["name"] = param_name
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
    if "schema" in param:
        spec_data = param["schema"]
        if "type" in spec_data and spec_data['type'] == 'array':
            param_data['payloadId'] = shortuuid.uuid()
            param_data['type'] = 'ezapi_ref'
            param_data['name'] = spec_data['items']['ezapi_ref'].split("/")[-1]
            param_data['ezapi_ref'] = spec_data['items']['ezapi_ref']
            param_data['ref'] = spec_data['items']['ezapi_ref'].split("/")[-1]
            param_data['isArray'] = True
        else:
            schema_name = spec_data['ezapi_ref'].split("/")[-1]

            param_data['payloadId'] = shortuuid.uuid()
            param_data['ezapi_ref'] = spec_data['ezapi_ref']
            param_data['name'] = schema_name
            param_data['type'] = 'ezapi_ref'
            param_data['ref'] = schema_name
            param_data['isArray'] = False
    # todo - other fields, user name is different than body
    param_name = param.get("name")
    param_desc = param.get("description")
    param_req = param.get("required", False)
    param_schema = param["schema"]  # required

    #param_data = param["schema"]
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
            #"content": resp_spec.get("schema"),
            "headers": resp_spec.get("headers"),
            "links": resp_spec.get("links")
        }
        if not obj["headers"]:
            obj["headers"] = []
        if not obj["links"]:
            obj["links"] = []
        empty_obj = {}

        if "schema" in resp_spec:
            spec_data = resp_spec["schema"]
            if "type" in spec_data and spec_data['type'] == 'array':
                empty_obj['payloadId'] = shortuuid.uuid()
                empty_obj['type'] = 'ezapi_ref'
                empty_obj['name'] = spec_data['items']['ezapi_ref'].split("/")[-1]
                empty_obj['ezapi_ref'] = spec_data['items']['ezapi_ref']
                empty_obj['ref'] = spec_data['items']['ezapi_ref'].split("/")[-1]
                empty_obj['isArray'] = True
            else:
                schema_name = spec_data['ezapi_ref'].split("/")[-1]

                empty_obj['payloadId'] = shortuuid.uuid()
                empty_obj['ezapi_ref'] = spec_data['ezapi_ref']
                empty_obj['name'] = schema_name
                empty_obj['type'] = 'ezapi_ref'
                empty_obj['ref'] = schema_name
                empty_obj['isArray'] = False

        if empty_obj:
            obj["content"] = empty_obj

        responnses.append(obj)
    return responnses


def parse_swagger(jsondata, projectid, db):
    try:
        print("Inside parsing swagger 2.0")

        # definitions -> componennts -> schemas
        project_orig = db.projects.find_one({"projectId": projectid})
        updated_resources_array = []
        curr_resources_array = project_orig.get("resources")

        jsonstr = json.dumps(jsondata)
        jsonstr = jsonstr.replace("#/definitions/", "#/components/schemas/")
        jsondata = json.loads(jsonstr)

        info_data = jsondata.get("info")
        path_data = jsondata.get("paths")
        components_data_2 = jsondata.get("definitions")
        components_data = {"schemas": components_data_2}

        all_paths, resource_array = extract_path_data(path_data)
        all_parameters = {}

        resource_document = {
            "resourceName": info_data["title"],
            "resourceId": str(uuid.uuid4()),
            "path": resource_array
        }
        path_collection = "resources"
        mongo.store_document(path_collection, resource_document, db)
        # Extract request, response data for individual path (endpoint) wise

        resource_obj = {
            "resource": resource_document["resourceId"]
        }
        updated_resources_array = curr_resources_array + [resource_obj]
        mongo.update_document(
            "projects",
            {"projectId": projectid},
            {
                "$set": {
                    "resources": updated_resources_array
                }
            },
            db,
        )

        for path in all_paths:
            endpoint = path["endpoint"]
            method = path["method"]
            schema_name = "null"
            id = path["id"]
            del path["id"]

            request_data = extract_request_data(
                path_data, endpoint, method, components_data
            )
            response_data = extract_response_data(
                path_data, endpoint, method, components_data
            )

            # fetching schemaName from response
            response_obj = response_data[0]
            if "content" in response_obj:
                if "name" in response_obj["content"] and "type" in response_obj["content"] and response_obj["content"][
                    "type"] == "ezapi_ref":
                    schema_name = response_obj["content"]["name"]

            # Extract all parameters
            for param in request_data["path"] + request_data["query"]:
                for k, v in param.items():
                    v["schemaName"] = schema_name
                    if k not in all_parameters:
                        all_parameters[k] = v
                        all_parameters[k]["header"] = False

            for param in request_data["header"]:
                for k, v in param.items():
                    v["schemaName"] = schema_name
                    if k not in all_parameters:
                        all_parameters[k] = v
                        all_parameters[k]["header"] = True

            path["requestData"] = request_data
            path["responseData"] = response_data
            print("pathh:", path)
            path_collection = "operationdatas"
            path_document = {
                "projectid": projectid,
                "id": id,
                "data": path,
            }
            mongo.store_document(path_collection, path_document, db)

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
