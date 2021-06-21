from pprint import pprint
import random

from api_designer.artefacts.ezfaker import generate_field_data
from api_designer.utils.common import *
from api_designer import config

from urllib.parse import urlencode

DATA_TYPE_LIST = ["integer", "number", "string", "boolean"]

# array
MIN_ITEMS = 1
MAX_ITEMS = 3

TESTCASE_COLLECTION = "testcases"
VIRTUAL_COLLECTION = "virtual"


class SchemaDeref:
    def __init__(self, schemas):
        self.schemas = schemas  # All Schema

    def deref_array(self, param_array):
        assert param_array["type"] == "array"
        res = {}

        array_items = param_array.get("items")
        array_items_type = array_items.get("type")

        if "ezapi_ref" in array_items:
            ref_schema = array_items["ezapi_ref"].split("/")[-1]
            ref_schema = self.schemas[ref_schema]
            res = self.deref_schema(ref_schema)

        else:
            res["type"] = array_items_type
            if array_items_type == "array":
                res["items"] = self.deref_array(array_items)

            elif array_items_type == "object":
                res["properties"] = self.deref_object(array_items)

        return res

    def deref_object(self, param_object):
        assert param_object["type"] == "object"
        res = {}

        if "properties" in param_object:  # only additionalProperties present
            for key, value in param_object["properties"].items():
                value_type = value.get("type")

                res[key] = {}

                if "ezapi_ref" in value:
                    ref_schema = value["ezapi_ref"].split("/")[-1]
                    ref_schema = self.schemas[ref_schema]
                    res[key] = self.deref_schema(ref_schema)

                else:
                    res[key]["type"] = value_type
                    if "format" in value:
                        res[key]["format"] = value["format"]

                    if value_type == "object":
                        res[key]["properties"] = self.deref_object(value)
                    elif value_type == "array":
                        res[key]["items"] = self.deref_array(value)

        return res

    def deref_schema(self, param_schema):
        res = {}

        if "allOf" in param_schema:
            all_schemas = param_schema["allOf"]
            res["type"] = "object"
            res["properties"] = {}

            for s in all_schemas:
                s_res = self.deref_schema(s)
                if "properties" in s_res:
                    res["properties"].update(s_res["properties"])

        else:
            st = param_schema.get("type")
            if "ezapi_ref" in param_schema:
                ref_schema = param_schema["ezapi_ref"].split("/")[-1]
                ref_schema = self.schemas[ref_schema]
                res = self.deref_schema(ref_schema)

            elif st == "object":
                res["type"] = "object"
                res["properties"] = self.deref_object(param_schema)

            elif st == "array":
                res["type"] = "array"
                res["items"] = self.deref_array(param_schema)

            else:
                res["type"] = st

        return res


class GenerateData:
    def __init__(self, schemas):
        self.schemas = schemas

    def generate_ref_data(self, param_ref):
        ref_schema = param_ref["ezapi_ref"]
        ref_schema = ref_schema.split("/")[-1]

        sd = SchemaDeref(self.schemas)
        v = sd.deref_schema(self.schemas[ref_schema])

        return self.generate_object_data(v)

    def generate_object_data(self, param_object):
        ret = {}
        for k, v in param_object["properties"].items():
            v_type = v.get("type")

            if v_type == "object" and "properties" in v:
                ret[k] = self.generate_object_data(v)
            elif v_type == "array" and "items" in v:
                ret[k] = self.generate_array_data(v, k)
            elif v_type in DATA_TYPE_LIST:
                ret[k] = generate_field_data(v, k)
            elif "ezapi_ref" in v:
                ret[k] = self.generate_ref_data(v)

        return ret

    def generate_array_data(self, param_array, key=None):
        ret = []

        v = param_array["items"]
        v_type = v.get("type")

        min_items = v.get("minItems", MIN_ITEMS)
        max_items = v.get("maxItems", MAX_ITEMS)
        arr_len = random.randint(min_items, max_items)

        for _ in range(arr_len):
            if v_type == "object" and "properties" in v:
                ret.append(self.generate_object_data(v))
            elif v_type == "array" and "properties" in v:
                ret.append(self.generate_array_data(v))
            elif v_type in DATA_TYPE_LIST:
                ret.append(generate_field_data(v, key))
            elif "ezapi_ref" in v:
                ret.append(self.generate_ref_data(v))

        # avoid duplicate entries
        ret = [i for n, i in enumerate(ret) if i not in ret[:n]]

        return ret

    def generate_param_data(self, param_list):
        ret = {}
        for param in param_list:
            for k, v in param.items():
                param_type = v.get("type")

                if param_type == "object" and "properties" in v:
                    ret[k] = self.generate_object_data(v)
                elif param_type == "array" and "items" in v:
                    ret[k] = self.generate_array_data(v, k)
                elif param_type in DATA_TYPE_LIST:
                    ret[k] = generate_field_data(v, k)
                elif "ezapi_ref" in v:
                    ret[k] = self.generate_ref_data(v)

        return ret

    def generate_body_data(self, body):
        ret = {}
        if not body:
            return ret
        body_type = body.get("type")

        if body_type == "object" and "properties" in body:
            ret = self.generate_object_data(body)
        elif body_type == "array" and "items" in body:
            ret = self.generate_array_data(body)
        elif "ezapi_ref" in body:
            ret = self.generate_ref_data(body)

        return ret

    def generate_request_data(self, request_data):
        if "body" not in request_data:
            request_data["body"] = {}

        payload = {
            "path": self.generate_param_data(request_data["path"]),
            "query": self.generate_param_data(request_data["query"]),
            "header": self.generate_param_data(request_data["header"]),
            # "form": generate_param_data(request_data["formData"]),
            "form": {},
            "body": self.generate_body_data(request_data["body"]),
        }
        return payload


def is_name_matched(name1, name2):
    name1 = sorted(string_split(name1))
    name2 = sorted(string_split(name2))
    return name1 == name2


def match_schema(s1, s2):
    if isinstance(s1, list):
        s1 = s1[0]
    if isinstance(s2, list):
        s2 = s2[0]

    matches = []
    for k1, v1 in s1.items():
        for k2, v2 in s2.items():
            if type(v1) == type(v2) and is_name_matched(k1, k2):
                matches.append((k1, k2))

    return matches


def match_request_response_data(testdata):
    request_data = testdata["inputData"]
    response_data = testdata["assertionData"]

    matched_schema_data = {
        "path": match_schema(request_data["path"], response_data),
        "query": match_schema(request_data["query"], response_data),
        "body": match_schema(request_data["body"], response_data),
    }

    for match_type, match_data in matched_schema_data.items():
        for m in match_data:
            if m[0] in request_data[match_type]:
                source_value = request_data[match_type][m[0]]
                if isinstance(response_data, list):
                    for i, _ in enumerate(response_data):
                        if m[1] in response_data[i]:
                            response_data[i][m[1]] = source_value
                else:
                    if m[1] in response_data:
                        response_data[m[1]] = source_value

    testdata["assertionData"] = response_data
    return testdata


def get_virtual_collection_data(testdata):
    virtual_service_data = {
        "projectid": testdata["projectid"],
        "api_ops_id": testdata["api_ops_id"],
        "httpMethod": testdata["method"],
        "headers": testdata["inputData"]["header"],
        "formData": testdata["inputData"]["form"],
        "requestBody": testdata["inputData"]["body"],
        "responseStatusCode": testdata["status"],
    }

    virtual_service_data["responseBody"] = testdata.get("assertionData", {})

    endpoint = testdata["endpoint"]

    if "{" in testdata["endpoint"]:
        regex = r"\{(.*?)\}"
        text_inside_paranthesis = re.findall(regex, endpoint)
        for xElm in text_inside_paranthesis:
            endpoint = endpoint.replace("{" + xElm + "}", "{" + xElm.lower() + "}")
    pathData = testdata["inputData"]["path"]
    queryData = testdata["inputData"]["query"]

    endpoint = endpoint.format(**pathData)
    tmp = urlencode(queryData)
    if tmp:
        endpoint += "?" + tmp
    virtual_service_data["endpoint"] = endpoint

    return virtual_service_data


def generate_artefacts(projectid, db):
    is_already_exist = db.testcases.find({"projectid": projectid})
    is_already_exist = list(is_already_exist)
    if is_already_exist and len(is_already_exist) > 0:
        return {"success": False, "message": "Already Exist", "status": 500}

    paths = db.operationdatas.find({"projectid": projectid})
    components = db.components.find_one({"projectid": projectid})

    paths = [x["data"] for x in paths]
    components = components["data"]
    schemas = components["schemas"]

    gd = GenerateData(schemas)

    all_testcases = []
    test_count = 0

    for path in paths:
        testdata = {
            "projectid": projectid,
            "api_ops_id": projectid,
            "endpoint": path.get("endpoint"),
            "method": path.get("method"),
            "resource": path.get("tags", []),
            "operation_id": path.get("operationId"),
            "test_case_name": path.get("operationId") + "__P",
            "description": "ok",
            "test_case_type": "F",
            "delete": False,
            "inputData": gd.generate_request_data(path["requestData"]),
            "status": None,
            "assertionData": None,
            "testcaseId": None,
        }

        for resp in path["responseData"]:
            testdata["status"] = resp["status_code"]
            testdata["assertionData"] = gd.generate_body_data(resp["content"])
            testdata["testcaseId"] = "test" + str(1 + test_count)

            test_copy = testdata.copy()

            if resp["status_code"] == "default" or resp["status_code"].startswith("2"):
                test_copy = match_request_response_data(test_copy)

            elif resp["status_code"] == "404":  # not found
                tmp = test_copy["endpoint"].split("/")
                for i, t in enumerate(tmp):
                    tmp[i] = "abc" + tmp[i]
                    break
                test_copy["endpoint"] = "/".join(tmp)
                test_copy["description"] = "uri not found"

            elif resp["status_code"] == "405":  # method not allowed
                test_copy["method"] = "head"
                test_copy["description"] = "method not allowed"

            all_testcases.append(test_copy)
            test_count += 1

    virtual_tests = [get_virtual_collection_data(x) for x in all_testcases]

    config.store_bulk_document(TESTCASE_COLLECTION, all_testcases, db)
    config.store_bulk_document(VIRTUAL_COLLECTION, virtual_tests, db)

    return {"success": True, "message": "ok", "status": 200}
