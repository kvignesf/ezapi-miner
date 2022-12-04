from sqlalchemy import column, false, null, true
from api_designer import mongo
from api_designer.utils.common import *
import shortuuid, uuid
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
_SCHEMA_TYPES = ["integer", "decimal", "string", "date", "time", "boolean"]
def extract_path_data(path_data):
    all_paths = []
    reso_path_array = []
    for path, path_spec in path_data.items():
        if path[0] == "/":
            path_obj = {}
            path_name = path
            path_obj["pathId"] = shortuuid.uuid()
            path_obj["pathName"] = path_name[1:]
            #print("path_name", path_name)
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
                        key: http_method_specs.get(key)
                        for key in http_method_specs.keys()
                        & {"tags", "summary", "description", "operationId"}
                    }

                    if "operationId" not in method_data:
                        method_data["operationId"] = http_method + " " + path

                    curr_path = merge_dict(curr_path, method_data)  # common.py

                    oper_obj["operationType"] = http_method.upper()
                    oper_obj["operationId"] = common_id
                    oper_obj["operationName"] = method_data["operationId"]
                    oper_obj["operationDescription"] = method_data.get("description","missingDescription")
                    oper_array.append(oper_obj)
    
                    all_paths.append(curr_path)
            path_obj["operations"] = oper_array
            reso_path_array.append(path_obj)
    return all_paths, reso_path_array

def generating_attribute_data_format(name, spec_data, custom_count):
    attribute_obj = {}
    empty_obj = {}

    cust_schema_name = "custom_schema_" + str(custom_count)
    cust_schema_ref = "#/components/schemas/"+ cust_schema_name
    cust_parent_name = "/"+ cust_schema_name

    attribute_obj['name'] = name
    attribute_obj['type'] = spec_data['type']
    attribute_obj['format'] = spec_data.get('format', "null")
    attribute_obj['required'] = spec_data.get('required',True)
    attribute_obj['level'] = 0
    attribute_obj['parent'] = "null"
    attribute_obj['is_child'] = True

    empty_obj['payloadId'] = shortuuid.uuid()
    empty_obj['name'] = name
    empty_obj['type'] = spec_data['type']
    empty_obj['description'] = "null"
    empty_obj['required'] = spec_data.get('required',True)
    empty_obj['possibleValues'] = "null"
    empty_obj['format'] = spec_data.get('format',"null")
    empty_obj['schemaRef'] = cust_schema_ref
    empty_obj['schemaName'] = cust_schema_name
    empty_obj['isArray'] = False
    empty_obj['parentName'] = cust_parent_name

    return attribute_obj, empty_obj, cust_schema_name

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
    param_data[param_name]["name"] = param_name 
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


def extract_body_content(body_content, components, custom_count):
    # check for 'type' or 'ezapi_ref' for dereferencing
    resp_spec = body_content["schema"]
    final_obj = {}
    properties_obj = {}
    flag = false
    schema_data_obj = {}
    if "properties" in resp_spec:
        components_schema = {
            "type": "object",
            "properties": {}
        }
        attributes_array = []
        resp_spec = resp_spec.get("properties")
        for name, spec_data in resp_spec.items():
            empty_obj = {}

            if "type" in spec_data and spec_data['type'] == 'array':
                empty_obj['payloadId'] = shortuuid.uuid()
                empty_obj['schemaName'] = name
                empty_obj['type'] = 'ezapi_ref'
                empty_obj['name'] = spec_data['items']['ezapi_ref'].split("/")[-1]
                empty_obj['ezapi_ref'] = spec_data['items']['ezapi_ref']
                empty_obj['ref'] = spec_data['items']['ezapi_ref'].split("/")[-1]
                empty_obj['isArray'] = True

            # elif "type" in spec_data and spec_data['type'] in _SCHEMA_TYPES:
            #     empty_obj['payloadId'] = shortuuid.uuid()
            #     empty_obj['name'] = name
            #     empty_obj['type'] = spec_data['type']
            #     empty_obj['description'] = "null"
            #     empty_obj['required'] = spec_data.get('required',True)
            #     empty_obj['possibleValues'] = "null"
            #     empty_obj['format'] = spec_data.get('format',"null")

            elif "type" in spec_data and spec_data['type'] in _SCHEMA_TYPES:
                flag = True

                if name not in components_schema["properties"]:
                    components_schema["properties"][name] = spec_data
                attribute_obj, empty_obj, cust_schema_name = generating_attribute_data_format(name, spec_data, custom_count)

            else:
                empty_obj['payloadId'] = shortuuid.uuid()
                empty_obj['schemaName'] = name
                empty_obj['type'] = 'ezapi_ref'
                empty_obj['name'] = spec_data['ezapi_ref'].split("/")[-1]
                empty_obj['ezapi_ref'] = spec_data['ezapi_ref']
                empty_obj['ref'] = spec_data['ezapi_ref'].split("/")[-1]
                empty_obj['isArray'] = False

            properties_obj[name] = empty_obj
            if attribute_obj:
                attributes_array.append(attribute_obj)

        if flag and cust_schema_name and components_schema["properties"] and components["schemas"]:
            components["schemas"][cust_schema_name] = components_schema

        if flag and cust_schema_name and attributes_array:
            schema_data_obj['name'] = cust_schema_name
            schema_data_obj['description'] = "null"
            schema_data_obj['size'] = len(attributes_array)
            schema_data_obj['max_depth'] = 1
            schema_data_obj['attributes'] = attributes_array

        if len(properties_obj) > 1:
            final_obj["type"] = "object"
            final_obj["properties"] = properties_obj
        elif len(properties_obj) == 1:
            final_obj = list(properties_obj.values())[0]

    else:
        if "type" in resp_spec and resp_spec['type'] == 'array':
            final_obj['type'] = 'ezapi_ref'
            final_obj['name'] = resp_spec['items']['ezapi_ref'].split("/")[-1]
            final_obj['ezapi_ref'] = resp_spec['items']['ezapi_ref']
            final_obj['ref'] = resp_spec['items']['ezapi_ref'].split("/")[-1]
            final_obj['isArray'] = True

        # elif "type" in spec_data and spec_data['type'] in _SCHEMA_TYPES:
        #         final_obj['payloadId'] = shortuuid.uuid()
        #         final_obj['name'] = name
        #         final_obj['type'] = spec_data['type']
        #         final_obj['description'] = "null"
        #         final_obj['required'] = spec_data.get('required',True)
        #         final_obj['possibleValues'] = "null"
        #         final_obj['format'] = spec_data.get('format',"null")

        else:
            schema_name = resp_spec['ezapi_ref'].split("/")[-1]

            final_obj['payloadId'] = shortuuid.uuid()
            final_obj['ezapi_ref'] = resp_spec['ezapi_ref']
            final_obj['name'] = schema_name
            final_obj['type'] = 'ezapi_ref'
            final_obj['ref'] = schema_name
            final_obj['isArray'] = False

    return final_obj, flag, components, schema_data_obj

def extract_form_content(form_content):
    return form_content["schema"]

def extract_request_body(request_body, components, custom_count):
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
    elif "application-json" in content:
        body_content = content["application-json"]
    elif "application/x-www-form-urlencoded" in content:
        body_content = content["application/x-www-form-urlencoded"]
    elif "application/octet-stream" in content:
        form_content = content["application/octet-stream"]

    if body_content:
        body_content, flag, updated_components, schema_data_obj = extract_body_content(body_content, components, custom_count)
    elif form_content:
        form_content = extract_form_content(form_content)

    body_content = body_content or []
    form_content = form_content or []

    return body_content, flag, updated_components, schema_data_obj, form_content


def extract_request_data(path_data, path, method, components, custom_count):
    all_request_params = {}
    updated_components = components
    schema_data_obj = {}

    flag = False
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
            flag,
            updated_components,
            schema_data_obj,
            all_request_params["formData"],
        ) = extract_request_body(
            request_body, components, custom_count
        )  # may contains ezapi_ref

    return all_request_params, flag, updated_components, schema_data_obj


# def extract_response_data(path_data, path, method, components):
#     # check for 'type' or 'ezapi_ref' for dereferencing if not None
#     responses = []
#     response_data = path_data[path][method].get("responses")
#     content_obj = {}
#     selected_columns = []


#     for resp, resp_spec in response_data.items():  # http_status and response object
#         resp_spec = resp_spec.get("content").get("application/json").get("schema")
#         if "type" in resp_spec and resp_spec['type'] == "array":
#             resp_spec = resp_spec.get("items")
#             content_obj['isArray'] = true

        

#         if "ezapi_ref" in resp_spec:
#             resp_ref = resp_spec["ezapi_ref"]
#             resp_ref = resp_ref.split("/")[-1]
#             content_obj['payloadId'] = null
#             content_obj['ezapi_ref'] = resp_ref
#             content_obj['sourceName'] = resp_ref
#             content_obj['key'] = null
#             content_obj['name'] = resp_ref
#             content_obj['type'] = "ezapi_table"


#             resp_spec = components.get('schemas')
#             resp_spec = resp_spec[resp_ref]
#             for col_name,col_ref in resp_spec['properties'].items():
#                 column_obj = {}
#                 column_obj['auto'] = false
#                 column_obj['name'] = col_name
#                 column_obj['required'] = false
#                 column_obj['type'] = col_ref['type']
#                 column_obj['format'] = col_ref['format']
#                 column_obj['keyType'] = null
#                 column_obj['sourceName'] = col_name
#                 column_obj['key'] = null
#                 column_obj['tableName'] = resp_ref
#                 column_obj['paramType'] = 'column'
#                 column_obj['payloadId'] = null
#                 selected_columns.append(column_obj)

#             content_obj['selectedColumns'] = selected_columns

            

#         obj = {
#             "status_code": resp,
#             "description": resp_spec.get("description"),
#             "content": content_obj,
#             "headers": resp_spec.get("headers"),
#             "links": resp_spec.get("links"),
#         }

#         responses.append(obj)
#     return responses

def extract_response_data(path_data, path, method, components, custom_count):
    # check for 'type' or 'ezapi_ref' for dereferencing if not None
    responses = []
    response_data = path_data[path][method].get("responses")
    flag = false
    schema_data_obj = {}

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

        if not obj["headers"]:
            obj["headers"] = []
        if not obj["links"]:
            obj["links"] = []

        if "content" in resp_spec:
            resp_spec = resp_spec.get("content")
            if "application/json" in resp_spec:
                resp_spec = resp_spec.get("application/json").get("schema")
            elif "application-json" in resp_spec:
                resp_spec = resp_spec.get("application-json").get("schema")

            final_obj = {}
            properties_obj = {}
            if "properties" in resp_spec:
                components_schema = {
                    "type": "object",
                    "properties": {}
                }
                attributes_array = []
                resp_spec = resp_spec.get("properties")
                for name, spec_data in resp_spec.items():
                    empty_obj = {}
                    attribute_obj = {}

                    if "type" in spec_data and spec_data['type'] == 'array':
                        empty_obj['payloadId'] = shortuuid.uuid()
                        empty_obj['schemaName'] = name
                        empty_obj['type'] = 'ezapi_ref'
                        empty_obj['name'] = spec_data['items']['ezapi_ref'].split("/")[-1]
                        empty_obj['ezapi_ref'] = spec_data['items']['ezapi_ref']
                        empty_obj['ref'] = spec_data['items']['ezapi_ref'].split("/")[-1]
                        empty_obj['isArray'] = True

                    # elif "type" in spec_data and spec_data['type'] in _SCHEMA_TYPES:
                    #     empty_obj['payloadId'] = shortuuid.uuid()
                    #     empty_obj['name'] = name
                    #     empty_obj['type'] = spec_data['type']
                    #     empty_obj['description'] = "null"
                    #     empty_obj['required'] = spec_data.get('required',True)
                    #     empty_obj['possibleValues'] = "null"
                    #     empty_obj['format'] = spec_data.get('format',"null")

                    elif "type" in spec_data and spec_data['type'] in _SCHEMA_TYPES:

                        flag = True

                        if name not in components_schema["properties"]:
                            components_schema["properties"][name] = spec_data
                        attribute_obj, empty_obj, cust_schema_name = generating_attribute_data_format(name, spec_data,
                                                                                                      custom_count)

                    else:
                        empty_obj['payloadId'] = shortuuid.uuid()
                        empty_obj['schemaName'] = name
                        empty_obj['type'] = 'ezapi_ref'
                        empty_obj['name'] = spec_data['ezapi_ref'].split("/")[-1]
                        empty_obj['ezapi_ref'] = spec_data['ezapi_ref']
                        empty_obj['ref'] = spec_data['ezapi_ref'].split("/")[-1]
                        empty_obj['isArray'] = False

                    properties_obj[name] = empty_obj
                    if attribute_obj:
                        attributes_array.append(attribute_obj)

                if flag and cust_schema_name and components_schema["properties"] and components["schemas"]:
                    components["schemas"][cust_schema_name] = components_schema

                if flag and cust_schema_name and attributes_array:
                    schema_data_obj['name'] = cust_schema_name
                    schema_data_obj['description'] = "null"
                    schema_data_obj['size'] = len(attributes_array)
                    schema_data_obj['max_depth'] = 1
                    schema_data_obj['attributes'] = attributes_array

                if len(properties_obj) > 1:
                    final_obj["type"] = "object"
                    final_obj["properties"] = properties_obj
                elif len(properties_obj) == 1:
                    final_obj = list(properties_obj.values())[0]

            else:
                if "type" in resp_spec and resp_spec['type'] == 'array':
                    final_obj['type'] = 'ezapi_ref'
                    final_obj['name'] = resp_spec['items']['ezapi_ref'].split("/")[-1]
                    final_obj['ezapi_ref'] = resp_spec['items']['ezapi_ref']
                    final_obj['ref'] = resp_spec['items']['ezapi_ref'].split("/")[-1]
                    final_obj['isArray'] = True

                # elif "type" in spec_data and spec_data['type'] in _SCHEMA_TYPES:
                #         final_obj['payloadId'] = shortuuid.uuid()
                #         final_obj['name'] = name
                #         final_obj['type'] = spec_data['type']
                #         final_obj['description'] = "null"
                #         final_obj['required'] = spec_data.get('required',True)
                #         final_obj['possibleValues'] = "null"
                #         final_obj['format'] = spec_data.get('format',"null")

                else:
                    schema_name = resp_spec['ezapi_ref'].split("/")[-1]

                    final_obj['payloadId'] = shortuuid.uuid()
                    final_obj['ezapi_ref'] = resp_spec['ezapi_ref']
                    final_obj['name'] = schema_name
                    final_obj['type'] = 'ezapi_ref'
                    final_obj['ref'] = schema_name
                    final_obj['isArray'] = False

            obj["content"] = final_obj

        if obj["content"] and (resp != "200" or not resp.startswith("2")):
            obj["content"] = obj["content"].get("application-json")

            if obj["content"]:
                obj["content"] = obj["content"].get("schema")

        responses.append(obj)
        return responses, flag, components, schema_data_obj

def update_schemas_components_rawspec(db, projectid, spec_filename, updated_components_data, schema_data_obj):
    if updated_components_data:
        spec_original = db.raw_spec.find_one({"projectid": projectid})
        spec_original["data"]["components"] = updated_components_data
        updated_data = spec_original["data"]

        mongo.update_document(
            "components",
            {"projectid": projectid},
            {
                "$set": {
                    "data": updated_components_data
                }
            },
            db,
        )

        mongo.update_document(
            "raw_spec",
            {"projectid": projectid},
            {
                "$set": {
                    "data": updated_data
                }
            },
            db,
        )
    if schema_data_obj:
        schema_document = {
                "filename": spec_filename,
                "projectid": projectid,
                "data": schema_data_obj
        }
        path_collection = "schemas"
        mongo.store_document(path_collection, schema_document, db)

def parse_openapi(jsondata, projectid, spec_filename, db):
    try:
        project_orig = db.projects.find_one({"projectId": projectid})
        updated_resources_array = []
        curr_resources_array = project_orig.get("resources")

        info_data = jsondata.get("info")
        path_data = jsondata.get("paths")
        components_data = jsondata.get("components")

        all_paths, resource_array = extract_path_data(path_data)
        all_parameters = {}

        
        resource_document = {
            "resourceName" : info_data["title"],
            "resourceId" : str(uuid.uuid4()),
            "path" : resource_array 
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
        custom_count = 1

        for path in all_paths:
            endpoint = path["endpoint"]
            method = path["method"]
            schema_name = "null"
            id = path["id"]
            del path["id"]
            request_data, flag, updated_components_data, schema_data_obj = extract_request_data(
                path_data, endpoint, method, components_data, custom_count
            )

            if flag:
                custom_count += 1
                update_schemas_components_rawspec(db, projectid, spec_filename, updated_components_data,
                                                  schema_data_obj)

            response_data, flag, updated_components_data, schema_data_obj = extract_response_data(
                path_data, endpoint, method, components_data, custom_count
            )

            if flag:
                custom_count += 1
                update_schemas_components_rawspec(db, projectid, spec_filename, updated_components_data,
                                                  schema_data_obj)
            
            #fetching schemaName from response
            response_obj = response_data[0]
            if "content" in response_obj:
                if "name" in response_obj["content"] and "type" in response_obj["content"] and response_obj["content"]["type"] == "ezapi_ref":
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


    