# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************


from api_designer import mongo
from api_designer.utils.schema_manager import SchemaDeref
from pprint import pprint

import os, sys


def transform_ds(tag_wise_attributes):
    res = []

    for k, v in tag_wise_attributes.items():
        tmp = {}
        tmp["tag"] = k
        tmp["data"] = v
        res.append(tmp)

    return res


def deref_body_data(body, schemas):
    sd = SchemaDeref(schemas)
    res = sd.deref_schema(body)
    return res


def deref_table_body_data(body):
    return body


def enhance_attributes(projectid, db, project_type, filename=None):
    try:
        paths = db.operationdatas.find({"projectid": projectid})
        project_data = db.projects.find_one({"projectId": projectid})

        project_resources = project_data.get("resources", [])
        project_resources = [x["resource"] for x in project_resources]

        resource_data = db.resources.find({"resourceId": {"$in": project_resources}})
        resource_data = list(resource_data)

        resource_dict = {}
        for r in resource_data:
            resource_path = r["path"]
            for rp in resource_path:
                resource_name = r["resourceName"]
                rp_endpoint = rp["pathName"]

                rp_operations = rp.get("operations", [])
                for rpo in rp_operations:
                    op_method = rpo["operationType"]
                    op_name = rpo["operationName"]
                    op_desc = rpo["operationDescription"]

                    resource_dict_key = op_method + "__" + rp_endpoint
                    resource_dict[resource_dict_key] = {
                        "tag": resource_name,
                        "summary": op_name,
                        "description": op_desc,
                    }

        paths = list(paths)
        if not paths:
            return False, "project data not found"

        if project_type == "db":
            pass
        else:
            components = db.components.find({"projectid": projectid})
            components = list(components)[0]["data"]
            all_schemas = components.get("schemas")  # An object

        tag_wise_attributes = {}

        for p in paths:
            path_data = p["data"]

            endpoint = path_data["endpoint"]
            method = path_data["method"]

            tmp_endpoint = endpoint.strip("/")
            tmp_endpoint = tmp_endpoint.split("/")[0]

            lookup_key = method.upper() + "__" + tmp_endpoint

            try:
                tag = resource_dict.get(lookup_key).get("tag")
            except:
                tag = "API Model"

            try:
                summary = resource_dict.get(lookup_key).get("summary")
            except:
                summary = method.upper() + " " + endpoint

            try:
                description = resource_dict.get(lookup_key).get("description")
            except:
                description = method.upper() + " " + endpoint

            tags = [tag]

            path_request_data = path_data["requestData"]
            path_param_data = (
                path_request_data["path"]
                + path_request_data["query"]
                + path_request_data["header"]
            )

            if project_type == "db":
                if "body" in path_request_data and path_request_data["body"]:
                    body_data = deref_table_body_data(path_request_data["body"])
                    path_param_data += [body_data]
            else:
                if "body" in path_request_data and path_request_data["body"]:
                    body_data = deref_body_data(path_request_data["body"], all_schemas)
                    path_param_data += [body_data]

            # tags = path_data.get("tags", ["API Model"])
            for t in tags:
                if t not in tag_wise_attributes:
                    tag_wise_attributes[t] = {}

                for param in path_param_data:
                    if "type" in param and param["type"] == "object":
                        param = param["properties"]

                    if "type" in param and param["type"] == "array":
                        param = param["items"]

                    if "type" not in param:  # Only flat keys
                        for k, v in param.items():
                            if k not in tag_wise_attributes[t]:
                                tag_wise_attributes[t][k] = []

                            tag_wise_attributes[t][k].append(
                                {
                                    "endpoint": endpoint,
                                    "method": method,
                                    "summary": summary,
                                    "description": description,
                                }
                            )
                    else:
                        k = "other"
                        if k not in tag_wise_attributes[t]:
                            tag_wise_attributes[t][k] = []

                        tag_wise_attributes[t][k].append(
                            {
                                "endpoint": endpoint,
                                "method": method,
                                "summary": summary,
                                "description": description,
                            }
                        )

        tag_wise_attributes = transform_ds(tag_wise_attributes)

        element_collction = "elements"
        element_document = {
            "projectid": projectid,
            "api_ops_id": projectid,
            "data": tag_wise_attributes,
        }
        mongo.store_document(element_collction, element_document, db)

        return True, "ok"

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print("Element Scoring Error - ", exc_type, fname, exc_tb.tb_lineno, str(e))

        return False, str(e)
