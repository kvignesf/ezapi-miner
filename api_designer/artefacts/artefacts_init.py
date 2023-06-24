# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************

from pprint import pprint
import json
import random
import string
import os, sys
import jwt

from itsdangerous import exc

# from api_designer.artefacts import generate_db_only
from api_designer.artefacts.ezfaker import generate_field_data
from api_designer.utils.common import *
from api_designer import mongo

from urllib.parse import urlencode

DATA_TYPE_LIST = ["integer", "number", "string", "boolean"]

# array
MIN_ITEMS = 1
MAX_ITEMS = 3

TESTCASE_COLLECTION = "testcases"
VIRTUAL_COLLECTION = "virtual"
SIM_TESTCASE_COLLECTION = "testcases_sim"
SIM_VIRTUAL_COLLECTION = "virtual_sim"
TESTRESULT_COLLECTION = "test_result"


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
    def __init__(self, schemas, is_response=False):
        self.schemas = schemas
        self.is_response = is_response
        self.payload = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 10))

    def set_response_flag(self, is_response):
        self.is_response = is_response

    def generate_ref_data(self, param_ref):
        ref_schema = param_ref["ezapi_ref"]
        ref_schema = ref_schema.split("/")[-1]

        sd = SchemaDeref(self.schemas)
        v = sd.deref_schema(self.schemas[ref_schema])

        return self.generate_object_data(v)

    def generate_object_data(self, param_object):
        ret = {}
        for k, v in param_object["properties"].items():
            is_required = v.get("required", True)
            toss_required = is_required or self.is_response
            toss = 1 if toss_required else random.randint(0, 1)

            v_type = v.get("type")

            if toss:
                generated = None
                if v_type == "object" and "properties" in v:
                    generated = self.generate_object_data(v)
                elif v_type == "array" and "items" in v:
                    generated = self.generate_array_data(v, k)
                elif v_type in DATA_TYPE_LIST:
                    generated = generate_field_data(v, k)
                elif "ezapi_ref" in v:
                    generated = self.generate_ref_data(v)

                if generated:
                    ret[k] = generated

        return ret

    def generate_array_data(self, param_array, key=None):
        ret = []

        is_required = param_array.get("required", True)
        toss_required = is_required or self.is_response
        toss = 1 if toss_required else random.randint(0, 1)

        v = param_array["items"]
        v_type = v.get("type")

        min_items = v.get("minItems", MIN_ITEMS)
        max_items = v.get("maxItems", MAX_ITEMS)
        arr_len = random.randint(min_items, max_items)

        if toss:
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
                possible_values = v.get("possibleValues")

                if possible_values:
                    random_item = random.choice(possible_values)
                    ret[k] = random_item
                elif param_type == "object" and "properties" in v:
                    ret[k] = self.generate_object_data(v)
                elif param_type == "array" and "items" in v:
                    ret[k] = self.generate_array_data(v, k)
                elif param_type in DATA_TYPE_LIST:
                    ret[k] = generate_field_data(v, k)
                elif "ezapi_ref" in v:
                    ret[k] = self.generate_ref_data(v)

        return ret

    def generate_authorization(self, auth):
        if auth and "authType" in auth and "tokenType" in auth:
            auth_type = auth["authType"]
            auth_token = auth["tokenType"]

            if auth_type == "Bearer Token" and auth_token == "JWT":
                token = jwt.encode({'payload': self.payload}, 'secret', algorithm='HS256')
                return token
        return None

    def generate_array_ref_data(self, param_ref):
        ref_schema = param_ref["ezapi_ref"]
        ref_schema = ref_schema.split("/")[-1]

        sd = SchemaDeref(self.schemas)
        v = sd.deref_schema(self.schemas[ref_schema])

        return self.generate_array_object_data(v)

    def generate_array_object_data(self, param_object):
        rets = []
        for i in range(2):
            ret = {}
            for k, v in param_object["properties"].items():
                is_required = v.get("required", True)
                print("requi:", is_required)
                toss_required = is_required or self.is_response
                toss = 1 if toss_required else random.randint(0, 1)

                v_type = v.get("type")

                if toss:
                    generated = None
                    if v_type == "object" and "properties" in v:
                        generated = self.generate_object_data(v)
                    elif v_type == "array" and "items" in v:
                        generated = self.generate_array_data(v, k)
                    elif v_type in DATA_TYPE_LIST:
                        generated = generate_field_data(v, k)
                    elif "ezapi_ref" in v:
                        generated = self.generate_ref_data(v)

                    if generated:
                        ret[k] = generated
            rets.append(ret)
        return rets


    def generate_body_data(self, body):
        ret = {}
        rets = []
        if not body:
            return ret
        body_type = body.get("type")
        isArray = body.get("isArray")

        if body_type == "object" and "properties" in body:
            ret = self.generate_object_data(body)
        elif body_type == "array" and "items" in body:
            ret = self.generate_array_data(body)
        elif "ezapi_ref" in body:
            if isArray:
                rets = self.generate_array_ref_data(body)
                return rets
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

        if "authorization" in request_data:
            token = self.generate_authorization(request_data["authorization"])
            if token:
                payload["header"]["authorization"] = token

        return payload


class GenerateTableData:
    def __init__(self, is_response=False):
        self.is_response = is_response
        self.payload = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 10))
        self.db = None
        self.projectid = ""

    def set_response_flag(self, is_response):
        self.is_response = is_response

    def extract_field_sample_data(self, param, param_key):
        ret = None
        param_type = param.get("paramType")
        db = self.db
        projectid = self.projectid

        if param_type and param_type == "column":
            table_collection = db.tables.find_one({"projectid": projectid, "key": param["key"]})
            table_data = table_collection["data"]

            column_list = table_data[0]
            column_index = None
            for index, column_name in enumerate(column_list):
                if column_name == param["sourceName"]:
                    column_index = index
                    break
            if len(table_data) > 1:
                random_sub_array = random.choice(table_data[1:])
                ret = random_sub_array[column_index]
                if ret:
                    return ret
        elif param.get("schemaName") == "global":
            possible_values_array = param.get("possibleValues")
            if possible_values_array:
                ret = random.choice(possible_values_array)
                return ret
        ret = generate_field_data(param, param_key)
        return ret

    def generate_ref_data(self, param_ref):
        selected_columns = param_ref.get("selectedColumns", [])
        ret = {}
        for s in selected_columns:
            sname = s["name"]
            ret[sname] = self.extract_field_sample_data(s, sname) #generate_field_data(s, sname)

        return ret

    def generate_array_ref_data(self, param_ref):
        selected_columns = param_ref.get("selectedColumns", [])
        rets = []
        for i in range(2):
            ret = {}
            for s in selected_columns:
                sname = s["name"]
                ret[sname] = self.extract_field_sample_data(s, sname) #generate_field_data(s, sname)
            rets.append(ret)
        return rets

    def generate_object_data(self, param_object):
        ret = {}
        for k, v in param_object["properties"].items():
            v_type = v.get("type")
            param_param_type = v.get("paramType")
            #is_child = v.get("isChild")
            parameter_type = v.get("schemaName")

            if param_param_type and param_param_type == "documentField":
                ret[k] = self.generate_documentField_data(v)
            elif v_type == "object" and "properties" in v:
                ret[k] = self.generate_object_data(v)
            elif v_type in DATA_TYPE_LIST:
                ret[k] = self.extract_field_sample_data(v, k) #generate_field_data(v, k)
            elif v_type == "ezapi_table":
                isArray = v.get("isArray")
                if isArray:
                    rets = self.generate_array_ref_data(v)
                    ret[k] = rets
                else:
                    ret[k] = self.generate_ref_data(v)
            elif v_type == "array" and parameter_type == "global":
                if len(v["possibleValues"]) == 1:
                    ret[k] = random.sample(v["possibleValues"], 1)
                elif len(v["possibleValues"]) > 1:
                    ret[k] = random.sample(v["possibleValues"], 2)
            elif v_type == "arrayOfObjects" or v_type == "array":
                rets = []
                for i in range(2):
                    obj_data = self.generate_object_data(v["items"])
                    rets.append(obj_data)
                ret[k] = rets
        return ret

    def generate_documentField_object_data(self, param_object):
        ret = {}
        for k, v in param_object.items():
            v_type = v.get("ezapi_type")

            if v_type == "object":
                ret[k] = self.generate_documentField_object_data(v["ezapi_object"])
            elif v_type == "array":
                ret[k] = self.generate_documentField_array_data(v["ezapi_array"])
            elif "ezapi_samples" in v and "ezapi_count" in v:
                samples_array = v["ezapi_samples"]
                sample_count = v["ezapi_count"]
                random_index = random.randint(0, sample_count - 1)
                ret[k] = samples_array[random_index]
        return ret

    def generate_documentField_array_data(self, param_object):
        ret = []
        param_type = param_object.get("ezapi_type")
        if param_type == "object":
            ret.append(self.generate_documentField_object_data(param_object["ezapi_object"]))
        elif param_type == "array":
            ret.append(self.generate_documentField_array_data(param_object))
        elif "ezapi_samples" in param_object and "ezapi_count" in param_object:
            samples_array = param_object["ezapi_samples"]
            random_index = random.randint(0, len(samples_array) - 1)
            ret = samples_array[random_index]
        elif "ezapi_array_samples" in param_object:
            array_samples_array = param_object["ezapi_array_samples"]
            random_index = random.randint(0, len(array_samples_array) - 1)
            ret = array_samples_array[random_index]
        return ret

    def generate_documentField_data(self, param_object):
        ret = None
        v_type = param_object.get("type")
        db = self.db
        projectid = self.projectid
        collection = db.mongo_collections.find_one({"projectid": projectid, "collection": param_object["tableName"]})
        attributes = collection["attributes"]
        if v_type == "object":
            ret = {}
            if param_object["ref"]:
                split_list = param_object["ref"].split(".")
                new_object = attributes[split_list[2]][split_list[3]]
                ret = self.generate_documentField_object_data(new_object)
            else:
                new_object = attributes[param_object["sourceName"]]["ezapi_object"]
                ret = self.generate_documentField_object_data(new_object)
        elif v_type == "array":
            ret = []
            if param_object["ref"]:
                split_list = param_object["ref"].split(".")
                new_object = attributes[split_list[2]][split_list[3]]
                ret = self.generate_documentField_array_data(new_object)
            else:
                new_object = attributes[param_object["sourceName"]]["ezapi_array"]
                ret = self.generate_documentField_array_data(new_object)
        else:
            if param_object.get("ref"):
                split_list = param_object["ref"].split(".")
                param = attributes[split_list[2]][split_list[3]]
                param = param[param_object["sourceName"]]
            else:
                param = attributes[param_object["sourceName"]]
            if "ezapi_samples" in param and "ezapi_count" in param:
                samples_array = param["ezapi_samples"]
                sample_count = param["ezapi_count"]
                random_index = random.randint(0, sample_count - 1)
                ret = samples_array[random_index]

        return ret

    def generate_param_data(self, param_list):
        ret = {}
        for param in param_list:
            for k, v in param.items():
                param_type = v.get("type")
                possible_values = v.get("possibleValues")
                param_param_type = v.get("paramType")
                is_child = v.get("isChild")

                if param_param_type and param_param_type == "documentField":
                    ret[k] = self.generate_documentField_data(v)
                elif possible_values and possible_values[0]:
                    random_item = random.choice(possible_values)
                    ret[k] = random_item
                elif param_type == "object" and "properties" in v:
                    ret[k] = self.generate_object_data(v)
                elif param_type in DATA_TYPE_LIST:
                    ret[k] = self.extract_field_sample_data(v, k) #generate_field_data(v, k)
                elif param_type == "ezapi_table":
                    ret[k] = self.generate_ref_data(v)

        return ret

    def generate_header_data(self, param_list):
        ret = {}
        for param in param_list:
            for k, v in param.items():
                param_type = v.get("type")

                if param_type == "object" and "properties" in v:
                    ret[k.lower()] = self.generate_object_data(v)
                elif param_type in DATA_TYPE_LIST:
                    ret[k.lower()] = self.extract_field_sample_data(v, k) #generate_field_data(v, k)
                elif param_type == "ezapi_table":
                    ret[k.lower()] = self.generate_ref_data(v)

        return ret

    def generate_authorization(self, auth):
        if auth and "authType" in auth and "tokenType" in auth:
            auth_type = auth["authType"]
            auth_token = auth["tokenType"]

            if auth_type == "Bearer Token" and auth_token == "JWT":
                token = jwt.encode({'payload': self.payload}, 'secret', algorithm='HS256')
                return token
        return None

    def generate_body_data(self, body):
        ret = {}
        if not body:
            return ret
        body_type = body.get("type")
        isArray = body.get("isArray")
        param_param_type = body.get("paramType")
        is_child = body.get("isChild")

        if param_param_type and param_param_type == "documentField":
            ret = self.generate_documentField_data(body)
        elif body_type == "object" and "properties" in body:
            ret = self.generate_object_data(body)
        elif body_type == "ezapi_table":
            if isArray:
                rets = self.generate_array_ref_data(body)
                return rets
            ret = self.generate_ref_data(body)

        return ret

    def generate_request_data(self, request_data):
        if "body" not in request_data:
            request_data["body"] = {}

        payload = {
            "path": self.generate_param_data(request_data["path"]),
            "query": self.generate_param_data(request_data["query"]),
            #"header": self.generate_param_data(request_data["header"]),
            "header": self.generate_header_data(request_data["header"]),
            # "form": generate_param_data(request_data["formData"]),
            "form": {},
            "body": self.generate_body_data(request_data["body"]),
        }

        if "authorization" in request_data:
            token = self.generate_authorization(request_data["authorization"])
            if token:
                payload["header"]["authorization"] = token

        return payload


def misspell_single_letter(text):
    if not text:
        return text
    text = list(text)
    for s in string.ascii_letters:
        if s in text:
            idx = text.index(s)
            text[idx] = "a" if text[idx] != "a" else "e"
            break
    return "".join(text)


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
            if isinstance(v2, dict):
                for k3, v3 in v2.items():
                    if type(v1) == type(v3) and is_name_matched(k1, k3):
                        matches.append((k1, k3))
            elif type(v1) == type(v2) and is_name_matched(k1, k2):
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
            if match_type == 'body' and isinstance(request_data[match_type], list) and isinstance(response_data, list):
                for i, _ in enumerate(response_data):
                    if m[0] in request_data[match_type][i] and m[1] in response_data[i]:
                        response_data[i][m[1]] = request_data[match_type][i][m[0]]
            elif m[0] in request_data[match_type]:
                source_value = request_data[match_type][m[0]]
                if isinstance(response_data, list):
                    for i, _ in enumerate(response_data):
                        if m[1] in response_data[i]:
                            response_data[i][m[1]] = source_value
                else:
                    for key, value in response_data.items():
                        if m[1] == key:
                            response_data[m[1]] = source_value
                        elif isinstance(value, dict):
                            if m[1] in value:
                                response_data[key][m[1]] = source_value
                    # if m[1] in response_data:
                    #     response_data[m[1]] = source_value

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
        "operation_id": testdata["operation_id"]
    }
    #print("virtualdata..1", testdata["inputData"]["body"])
    virtual_service_data["responseBody"] = testdata.get("assertionData", {})
    respbodyList = []

    if isinstance(testdata["assertionData"], dict):
        respbodyList.append(testdata["assertionData"])
        virtual_service_data["responseBody"] = respbodyList

    reqbodyList = []
    if isinstance(testdata["inputData"]["body"], dict):
        if virtual_service_data.get("requestBody") == {}:
            #print("reqbody..", testdata.get("requestBody"), [])
            virtual_service_data["requestBody"] = reqbodyList
        else:
            reqbodyList.append(testdata["inputData"]["body"])
            virtual_service_data["requestBody"] = reqbodyList
    endpoint = testdata["endpoint"]

    # update path parameter
    path_parameters = testdata["inputData"]["path"]
    if path_parameters:
        endpoint = endpoint.format(**path_parameters)

    # update query parameter
    query_parameters = testdata["inputData"]["query"]
    if query_parameters:
        endpoint = endpoint + "?" + urlencode(query_parameters)

    # if "{" in testdata["endpoint"]:
    #     regex = r"\{(.*?)\}"
    #     text_inside_paranthesis = re.findall(regex, endpoint)
    #     for xElm in text_inside_paranthesis:
    #         endpoint = endpoint.replace("{" + xElm + "}", "{" + xElm.lower() + "}")

    # pathData = testdata["inputData"]["path"]
    # queryData = testdata["inputData"]["query"]

    # endpoint = endpoint.format(**pathData)
    # tmp = urlencode(queryData)
    # if tmp:
    #     endpoint += "?" + tmp

    virtual_service_data["endpoint"] = endpoint
    return virtual_service_data


def getCountByKey(input_payload):
    tc_suffix = ""
    elmntsNmbr = 0

    for k, v in input_payload.items():
        if len(v) > 0:
            if isinstance(v, list):
                v = v[0]
            nmbrelmnts = len(v)
            for a, b in v.items():
                if isinstance(b, dict):
                    nmbrelmnts = len(b)

            tc_suffix = tc_suffix + k[0] + str(nmbrelmnts)
            elmntsNmbr = elmntsNmbr + nmbrelmnts
    return elmntsNmbr

def check_keywrd_exists(keywrd, db, dbtype):
    masterkeywrds = db.db_key_words.find({"dbtype": dbtype})
    message = "failure"
    for mstrdata in masterkeywrds:
        #print(mstrdata["keywords"])
        for i in range(len(mstrdata["keywords"])):
            #print(mstrdata["keywords"][i])
            if (mstrdata["keywords"][i]).lower() == keywrd.lower():
                #print(keywrd.lower())
                message = "success"

    return message




def generate_artefacts(projectid, db):
    print("Inside Artefacts Generator")
    try:
        # Remove esisting testcases
        try:
            db.testcases.remove({"projectid": projectid})
        except:
            pass

        try:
            db.virtual.remove({"projectid": projectid})
        except:
            pass

        try:
            db.test_result.remove({"projectid": projectid})
        except:
            pass

        project_data = db.projects.find_one({"projectId": projectid})
        try:
            filename = project_data.get("apiSpec")
            filename = filename[0]["name"]
        except:
            filename = None

        project_data = db.projects.find_one({"projectId": projectid})
        project_type = project_data.get("projectType", None)

        if not project_data or not project_type:
            return {
                "success": False,
                "status": 404,
                "message": "project data or project type not found",
            }

        try:
            filename = project_data.get("apiSpec")
            filename = filename[0]["name"]
        except:
            filename = None

        paths = db.operationdatas.find({"projectid": projectid})
        paths = list(paths)

        if not paths:
            return {
                "success": False,
                "message": "project data not found",
                "status": 404,
            }
        paths = [x["data"] for x in paths]

        if project_type == "db":
            gd = GenerateTableData()
            gd.db = db
            gd.projectid = projectid
        else:
            components = db.components.find_one({"projectid": projectid})
            components = components["data"]
            schemas = components["schemas"]
            gd = GenerateData(schemas)

        testcase_result = {
            "api_ops_id": projectid,
            "projectid": projectid,
            "run1": {},
            "run2": {},
            "run3": {},
        }

        all_testcases = []
        test_count = 0

        for path in paths:
            paramswobracs = ""
            testdata = {
                "projectid": projectid,
                "api_ops_id": projectid,
                "filename": filename,
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
                "mock": True
            }
            if ("{" in path.get("endpoint")) and ("}" in path.get("endpoint")):
                pathparams = re.findall(r'\{.*?\}', path.get("endpoint"))
                endpoint_orig = path.get("endpoint")
                # print(path.get("endpoint").format(**testdata["inputData"]["path"]))
                testdata["endpoint"] = path.get("endpoint").format(**testdata["inputData"]["path"])
                testdata["endpoint_orig"] = endpoint_orig
            gd.set_response_flag(True)

            for resp in path["responseData"]:
                testdata["status"] = resp["status_code"]
                testdata["assertionData"] = (
                    gd.generate_body_data(resp["content"]) if "content" in resp else {}
                )
                testdata["testcaseId"] = "test" + str(1 + test_count)

                test_copy = testdata.copy()
                generated = False

                if resp["status_code"] == "default" or resp["status_code"].startswith(
                    "2"
                ):
                    test_copy = match_request_response_data(test_copy)
                    generated = True

                elif resp["status_code"] == "400":
                    # Deceptive Request
                    tmp = test_copy["endpoint"].split("/")
                    for i, t in enumerate(tmp):
                        tmp[i] = "%/" + tmp[i]
                        break
                    test_copy["endpoint"] = "/".join(tmp)
                    test_copy["description"] = "Deceptive request"
                    generated = True

                    # Bad Request (Missing Parameter)
                    # todo

                elif resp["status_code"] == "404":  # not found
                    tmp = test_copy["endpoint"].split("/")
                    tmp[0] = misspell_single_letter(tmp[0])
                    # for i, t in enumerate(tmp):
                    #     tmp[i] = "abc" + tmp[i]
                    #     break
                    test_copy["endpoint"] = "/".join(tmp)
                    test_copy["description"] = "misspelled uri, not found"
                    generated = True

                elif resp["status_code"] == "405":  # method not allowed
                    test_copy["method"] = "head"
                    test_copy["description"] = "method not allowed"
                    generated = True

                if generated:
                    if filename:
                        suffix = getCountByKey(testdata["inputData"])
                        suffix_end = " datasets" if suffix > 1 else " dataset"
                        test_copy["test_case_name"] = (
                            "Validate "
                            + resp["status_code"]
                            + " response for "
                            + path.get("operationId")
                            + " of "
                            + filename.split(".")[0]
                            + " API using "
                            + str(suffix)
                            + suffix_end
                        )
                    all_testcases.append(test_copy)
                    test_count += 1

        virtual_tests = [
            get_virtual_collection_data(x)
            for x in all_testcases
            if x["status"][0] == "2"
        ]

        mongo.store_bulk_document(TESTCASE_COLLECTION, all_testcases, db)
        mongo.store_bulk_document(VIRTUAL_COLLECTION, virtual_tests, db)
        mongo.store_document(TESTRESULT_COLLECTION, testcase_result, db)

        tbl_dbdata_recs = db.table_dbdata_map.find({"projectid": projectid})
        try:
            for tbl_dbdata in tbl_dbdata_recs:
                proj = tbl_dbdata['_id']
                mongo.update_document(
                    "table_dbdata_map",
                    {"_id": proj},
                    {
                        "$set": {
                            "dbdata_recordindex": 0
                        }
                    },
                    db
                )
        except:
            retMsg = "unable to update table data map"
            return {"success": False, "message": retMsg, "status": 404}

        return {"success": True, "message": "ok", "status": 200}
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print("Artefacts Error - ", exc_type, fname, exc_tb.tb_lineno, str(e))

        print("Artefacts Generator Error - ", str(e))
        return {"success": False, "message": str(e), "status": 500}


def get_sim_virtual_collection_data(testdata):


    virtual_service_data = {
        "projectid": testdata["projectid"],
        "api_ops_id": testdata["api_ops_id"],
        "httpMethod": testdata["method"],
        "headers": testdata["inputData"]["header"],
        "formData": testdata["inputData"]["form"],
        "requestBody": testdata["inputData"]["body"],
        "responseStatusCode": testdata["status"],
        "operation_id": testdata["operation_id"],
        "operationId":testdata["operationId"]
    }
    #print("virtualdata..1", testdata["inputData"]["body"])
    virtual_service_data["responseBody"] = testdata.get("assertionData", {})
    respbodyList = []

    if isinstance(testdata["assertionData"], list):
        #respbodyList.append(testdata["assertionData"])
        virtual_service_data["responseBody"] = testdata.get("assertionData")

    reqbodyList = []
    if isinstance(testdata["inputData"]["body"], dict):
        if virtual_service_data.get("requestBody") == {}:
            #print("reqbody..", testdata.get("requestBody"), [])
            virtual_service_data["requestBody"] = reqbodyList
        else:
            #reqbodyList.append(testdata["inputData"]["body"])
            virtual_service_data["requestBody"] = testdata["inputData"]["body"]
    elif isinstance(testdata["inputData"]["body"], list):
        virtual_service_data["requestBody"] = testdata["inputData"]["body"]

    endpoint = testdata["endpoint"]

    # update path parameter
    path_parameters = testdata["inputData"]["path"]
    if path_parameters:
        endpoint = endpoint.format(**path_parameters)

    # update query parameter
    query_parameters = testdata["inputData"]["query"]
    if query_parameters:
        endpoint = endpoint + "?" + urlencode(query_parameters)

    # if "{" in testdata["endpoint"]:
    #     regex = r"\{(.*?)\}"
    #     text_inside_paranthesis = re.findall(regex, endpoint)
    #     for xElm in text_inside_paranthesis:
    #         endpoint = endpoint.replace("{" + xElm + "}", "{" + xElm.lower() + "}")

    # pathData = testdata["inputData"]["path"]
    # queryData = testdata["inputData"]["query"]

    # endpoint = endpoint.format(**pathData)
    # tmp = urlencode(queryData)
    # if tmp:
    #     endpoint += "?" + tmp

    virtual_service_data["endpoint"] = endpoint
    return virtual_service_data


def generate_simulation_artefacts(projectid, db, operationId):
    print("Inside Artefacts Generator")
    try:
        # Remove esisting testcases
        try:
            db.testcases_sim.remove({"projectid": projectid})
        except:
            pass

        try:
            db.virtual_sim.remove({"projectid": projectid})
        except:
            pass

        project_data = db.projects.find_one({"projectId": projectid})
        try:
            filename = project_data.get("apiSpec")
            filename = filename[0]["name"]
        except:
            filename = None

        project_data = db.projects.find_one({"projectId": projectid})
        project_type = project_data.get("projectType", None)
        #resourceId = project_type.get("resources.resource", None)

        if not project_data or not project_type:
            return {
                "success": False,
                "status": 404,
                "message": "project data or project type not found",
            }

        try:
            filename = project_data.get("apiSpec")
            filename = filename[0]["name"]
        except:
            filename = None
        if operationId:
            paths = db.operationdatas.find({"projectid": projectid, "id": operationId})
        else:
            paths = db.operationdatas.find({"projectid": projectid})
        paths = list(paths)

        if not paths:
            return {
                "success": False,
                "message": "project data not found",
                "status": 404,
            }
        paths = [[x["data"], x["id"]] for x in paths]




        if project_type == "db" or project_type == "noinput" or project_type == "aggregate":
            gd = GenerateTableData()
            gd.db = db
            gd.projectid = projectid
        else:
            components = db.components.find_one({"projectid": projectid})
            components = components["data"]
            schemas = components["schemas"]
            gd = GenerateData(schemas)

        testcase_result = {
            "api_ops_id": projectid,
            "projectid": projectid,
            "run1": {},
            "run2": {},
            "run3": {},
        }

        all_testcases = []
        test_count = 0

        #resourceData = db.resources.find_one({"resourceId": resourceId})
        #resourcePaths = [z["path"] for z in resourceData]

        #for resourcePath in resourcePaths:
        #    resourcePath.get("operations")

        for xpath in paths:
            path = xpath[0]

            paramswobracs = ""
            testdata = {
                "projectid": projectid,
                "api_ops_id": projectid,
                "filename": filename,
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
                "mock": True,
                "operationId": xpath[1]
            }
            if ("{" in path.get("endpoint")) and ("}" in path.get("endpoint")):
                pathparams = re.findall(r'\{.*?\}', path.get("endpoint"))
                endpoint_orig = path.get("endpoint")
                # print(path.get("endpoint").format(**testdata["inputData"]["path"]))
                testdata["endpoint"] = path.get("endpoint").format(**testdata["inputData"]["path"])
                testdata["endpoint_orig"] = endpoint_orig
            gd.set_response_flag(True)

            for resp in path["responseData"]:
                testdata["status"] = resp["status_code"]
                testdata["assertionData"] = (
                    gd.generate_body_data(resp["content"]) if "content" in resp else {}
                )
                testdata["testcaseId"] = "test" + str(1 + test_count)

                test_copy = testdata.copy()
                generated = False

                if resp["status_code"] == "default" or resp["status_code"].startswith(
                    "2"
                ):
                    test_copy = match_request_response_data(test_copy)
                    generated = True

                elif resp["status_code"] == "400":
                    # Deceptive Request
                    tmp = test_copy["endpoint"].split("/")
                    for i, t in enumerate(tmp):
                        tmp[i] = "%/" + tmp[i]
                        break
                    test_copy["endpoint"] = "/".join(tmp)
                    test_copy["description"] = "Deceptive request"
                    generated = True

                    # Bad Request (Missing Parameter)
                    # todo

                elif resp["status_code"] == "404":  # not found
                    tmp = test_copy["endpoint"].split("/")
                    tmp[0] = misspell_single_letter(tmp[0])
                    # for i, t in enumerate(tmp):
                    #     tmp[i] = "abc" + tmp[i]
                    #     break
                    test_copy["endpoint"] = "/".join(tmp)
                    test_copy["description"] = "misspelled uri, not found"
                    generated = True

                elif resp["status_code"] == "405":  # method not allowed
                    test_copy["method"] = "head"
                    test_copy["description"] = "method not allowed"
                    generated = True

                if generated:
                    if filename:
                        suffix = getCountByKey(testdata["inputData"])
                        suffix_end = " datasets" if suffix > 1 else " dataset"
                        test_copy["test_case_name"] = (
                            "Validate "
                            + resp["status_code"]
                            + " response for "
                            + path.get("operationId")
                            + " of "
                            + filename.split(".")[0]
                            + " API using "
                            + str(suffix)
                            + suffix_end
                        )
                    all_testcases.append(test_copy)
                    test_count += 1


        virtual_tests = [get_sim_virtual_collection_data(x) for x in all_testcases if x["status"][0] == "2"]

        mongo.store_bulk_document(SIM_TESTCASE_COLLECTION, all_testcases, db)
        mongo.store_bulk_document(SIM_VIRTUAL_COLLECTION, virtual_tests, db)
        #mongo.store_document(TESTRESULT_COLLECTION, testcase_result, db)

        return {"success": True, "message": "ok", "status": 200}
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print("Artefacts Error - ", exc_type, fname, exc_tb.tb_lineno, str(e))

        print("Artefacts Generator Error - ", str(e))
        msg = str(e)
        if (msg.__contains__("object has no attribute 'items'")):
            msg = "object cannot be empty and must have at least one attribute"
        elif (msg.__contains__("'properties'")):
            msg = "array of objects cannot be empty and must have at least one attribute"

        return {"success": False, "message": msg, "status": 500}