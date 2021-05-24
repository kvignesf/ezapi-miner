import os, sys
from api_designer.spec_parser.schema_manager import crawl_schema

from api_designer import config

_HTTP_VERBS = set(["get", "put", "post", "delete", "options", "head", "patch"])
_PARAMETER_TYPES = set(
    ["path", "query", "header", "body", "formData", "cookie"]
)  # cookie - openapi 3.0

_SCHEMA_FIELDS = [
    "enum",
    "default",
    "pattern",
    "maximum",
    "minimum",
    "minLength",
    "maxLength",
    "minItems",
    "maxItems",
]
_PARAMETER_TYPES = set(["path", "query", "header", "cookie", "body", "formData"])


def extract_path_data(path_data):
    all_paths = []

    for path, path_spec in path_data.items():
        if path[0] == "/":
            curr_path = {}
            curr_path["path"] = path
            curr_path["methods"] = []
            curr_path["methods_definition"] = []

            for http_method, http_method_specs in path_spec.items():
                if http_method in _HTTP_VERBS:
                    curr_path["methods"].append(http_method)
                    method_data = {
                        key: http_method_specs[key]
                        for key in http_method_specs.keys()
                        & {
                            "tags",
                            "summary",
                            "description",
                            "operationId",
                            "consumes",
                            "produces",
                        }
                    }
                    # todo: Get apiops_description
                    curr_path["methods_definition"].append(method_data)

            all_paths.append(curr_path)
    return all_paths


def extract_param_data(param, components):
    if "ezapi_ref" in param:
        try:
            param_ref = param["ezapi_ref"]

            # Ignore '#', 'components' prefix
            param_ref = param_ref.split("/")[2:]

            param = components
            for pr in param_ref:
                param = param[pr]
        except Exception as e:
            print("*Error - Parameter dereference - ", str(e))
            param = None

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

    if "type" in param_schema and param_schema["type"] == "array":
        pass  # todo

    elif "type" in param_schema and param_schema["type"] == "object":
        pass  # todo

    return param_in, param_data


def extract_body_content(body_content):
    # check for 'type' or 'ezapi_ref' for dereferencing
    return body_content["schema"]


def extract_request_body(request_body, components):
    if "ezapi_ref" in request_body:  # body refers to requestBodies
        body_ref = request_body["ezapi_ref"]
        body_ref = body_ref.split("/")[2:]

        request_body = components
        for br in body_ref:
            request_body = request_body[br]

    body_content = request_body["content"]
    if "application/json" in body_content:
        body_content = body_content["application/json"]
    elif "application/x-www-form-urlencoded" in body_content:
        body_content = body_content["application/x-www-form-urlencoded"]
    elif "application/octet-stream" in body_content:
        body_content = body_content["application/octet-stream"]

    body_content = extract_body_content(body_content)
    return body_content


def extract_request_data(path_data, path, method, components):
    all_request_params = {}
    for p in _PARAMETER_TYPES:
        all_request_params[p] = []

    parameters = path_data[path][method].get("parameters", [])
    request_body = path_data[path][method].get("requestBody")

    # A parameter data can be in path, query, header, formData, or cookie
    for param in parameters:
        param_in, param_data = extract_param_data(param, components)

        if param_in in _PARAMETER_TYPES:
            all_request_params[param_in].append(param_data)

    if request_body:
        all_request_params["body"] = extract_request_body(
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
            obj["content"] = obj["content"].get("application/json")

            if obj["content"]:
                obj["content"] = obj["content"].get("schema")

        responses.append(obj)
    return responses


def extract_component_data(component_data):
    return component_data


def parse_openapi(jsondata, api_design_id, spec_filename, db):
    print("Inside parsing openapi 3.0")

    try:
        fullspec_collection = "raw_spec"
        fullspec_document = {
            "filename": spec_filename,
            "api_design_id": api_design_id,
            "data": jsondata,
        }
        config.store_document(fullspec_collection, fullspec_document, db)

        info_data = jsondata.get("info")
        apiinfo_collection = "api_info"
        apiinfo_document = {
            "filename": spec_filename,
            "api_design_id": api_design_id,
            "data": info_data,
        }
        config.store_document(apiinfo_collection, apiinfo_document, db)

        servers_data = jsondata.get("servers")
        server_collection = "servers"
        servers_document = {
            "filename": spec_filename,
            "api_design_id": api_design_id,
            "data": servers_data,
        }
        config.store_document(server_collection, servers_document, db)

        path_data = jsondata.get("paths")
        components_data = jsondata.get("components")

        security_data = jsondata.get("security")
        security_collection = "security"
        security_document = {
            "filename": spec_filename,
            "api_design_id": api_design_id,
            "data": security_data,
        }
        config.store_document(security_collection, security_document, db)

        tags_data = jsondata.get("tags")
        tags_collection = "tags"
        tags_document = {
            "filename": spec_filename,
            "api_design_id": api_design_id,
            "data": tags_data,
        }
        config.store_document(tags_collection, tags_document, db)

        externalDocs_data = jsondata.get("externalDocs")
        externalDocs_collection = "externalDocs"
        externalDocs_document = {
            "filename": spec_filename,
            "api_design_id": api_design_id,
            "data": externalDocs_data,
        }
        config.store_document(externalDocs_collection, externalDocs_document, db)

        # v3.1
        # jsonSchemaDialect_data = jsondata.get("jsonSchemaDialect")
        # webhooks_data = jsondata.get("webhooks")

        all_paths = extract_path_data(path_data)
        for path in all_paths:
            path_collection = "paths"
            path_document = {
                "filename": spec_filename,
                "api_design_id": api_design_id,
                "data": path,
            }
            config.store_document(path_collection, path_document, db)

        # Extract request, response data for individual path (endpoint) wise
        for path in all_paths:
            endpoint = path["path"]
            methods = path["methods"]

            for meth in methods:
                request_data = extract_request_data(
                    path_data, endpoint, meth, components_data
                )
                response_data = extract_response_data(
                    path_data, endpoint, meth, components_data
                )

                request_collection = "requests"
                request_document = {
                    "filename": spec_filename,
                    "api_design_id": api_design_id,
                    "path": endpoint,
                    "method": meth,
                    "data": request_data,
                }
                config.store_document(request_collection, request_document, db)

                response_collection = "responses"
                response_document = {
                    "filename": spec_filename,
                    "api_design_id": api_design_id,
                    "path": endpoint,
                    "method": meth,
                    "data": response_data,
                }
                config.store_document(response_collection, response_document, db)

        component_data = extract_component_data(components_data)
        component_collection = "components"
        component_document = {
            "filename": spec_filename,
            "api_design_id": api_design_id,
            "data": components_data,
        }
        config.store_document(component_collection, component_document, db)

        schemas = components_data["schemas"]
        crawled_schemas = crawl_schema(schemas)

        for cs in crawled_schemas:
            schema_collection = "schemas"
            schema_document = {
                "filename": spec_filename,
                "api_design_id": api_design_id,
                "data": cs,
            }
            config.store_document(schema_collection, schema_document, db)

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

    print("Spec parser res - ", res)
    return res
