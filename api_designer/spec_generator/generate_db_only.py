# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************


from importlib.resources import is_resource                                           
from pprint import pprint
from sqlalchemy import false, true                                  
from api_designer import mongo
from api_designer.spec_generator.mongo_generator import MongoGenerator


PATH_DESCRIPTION_KEYS = ["tags", "summary", "description", "operationId"]
PARAMETER_KEY = "parameters"
BODY_KEY = "requestBody"
RESPONSE_KEY = "responses"
SECURITY_KEY = "security"  # todo: later

PATH_PARAMETERS = ["path", "query", "header", "cookie"]
PARAMETER_KEYS = ["description", "required"]
PARAMETER_SCHEMA_KEYS = [
    "type",
    "format",
    "enum",
    "default",
    "minimum",
    "maximum",
    "minLength",
    "maxLength",
    "pattern",
]

RESPONSE_KEYS = ["description", "headers", "content", "links"]
SPEC_COLLECTION = "genspec"


def dict_merge(dict1, dict2):
    return dict1.update(dict2)


class SpecGenerator:
    def __init__(self, projectid, db):
        self.projectid = projectid
        self.db = db

        self.generated_schema_count = 0
        self.generated_request_count = 0
        self.generated_response_count = 0

        # generated data
        self.paths = {}

        self.parameters = {}
        self.headers = {}
        self.requestBodies = {}
        self.responses = {}
        self.schemas = {}
        self.components = {}
        self.schema_counter = {}
        self.schema_data = {}

        self.spec = {}
        self.schemas_list = set()

        self.operations = self.db.operationdatas.find({"projectid": self.projectid})
        self.tables = self.db.tables.find({"projectid": self.projectid})
        self.operations = [x["data"] for x in self.operations]

        db_collection = self.db["mongo_collections"]
        schema_data = db_collection.find({"projectid": self.projectid})
        schema_data = list(schema_data)
        for sd in schema_data:
            self.schema_data[sd["collection"]] = sd["attributes"]

        print("DB Data fetched")

    def generate_field(self, field_data):
        ret = {}
        name = field_data["name"]
        values = {
            "type": field_data["type"],
            "format": field_data.get("format", None),
            # "required": field_data.get("required", True), # no required field inside schema objects
        }
        
        if field_data["type"] == "array":
           values = { 
            "type": field_data["type"],
            "format": field_data.get("format", None),
            "items" : {
                     "type":"integer"#This need to be changed in future, for now it is hardcoded.
                }
            } 
        if not values["format"]:
            del values["format"]
        if values["type"] == "string":
            if "format" in values:
                del values["format"]
        ret[name] = values
        return ret
        
    def generate_array_table(self, table_data, request_body):   # convert it into a schema
        table_key = table_data["name"]
        selected_columns = table_data.get("selectedColumns", [])
        ret = {
            table_key: {"type": "array", "items": {"type":"object", "properties" : {}}},
        }
        required_array = []

        if selected_columns:
            for s in selected_columns:
                sk = s["name"]
                is_required = s["required"]

                sv = {
                    "type": s["type"],
                    "format": s.get("format", None),
                    # "required": s.get("required", True), # no required field inside schema objects
                }

                if is_required:
                    required_array.append(s["name"])

                if not sv["format"]:
                    del sv["format"]
                if sv["type"] == "string":
                    del sv["format"]

                ret[table_key]["items"]["properties"][sk] = sv
            if request_body:
                ret[table_key]["items"]["required"] = required_array
            
        return ret
    def generate_table(self, table_data, request_body = True,is_object = False):   # convert it into a schema
        ret = {}
        # ret2 = {}
        table_key = table_data["name"]
        selected_columns = table_data.get("selectedColumns", [])
        required_array = []
        # is_array = table_data["isArray"]

        # if is_array:
        #     ret = {
        #         table_key: {"type": "array", "items": {}},
        #     }
        #     if selected_columns:
        #         for s in selected_columns:
        #             sk = s["name"]
        #             sv = {
        #                 "type": s["type"],
        #                 "format": s.get("format", None),
        #                 # "required": s.get("required", True), # no required field inside schema objects
        #             }
        #             if not sv["format"]:
        #                 del sv["format"]

        #             ret[table_key]["items"][sk] = sv
        
        if selected_columns:
            ret = {
                table_key: {"type": "object", "properties": {}},
            }
            for s in selected_columns:
                sk = s["name"]
                is_required = s["required"]                                           
                sv = {
                    "type": s["type"],
                    "format": s.get("format", None),
                    # "required": s.get("required", True), # no required field inside schema objects
                }
                if is_required:
                    required_array.append(s["name"])                                                  
                if not sv["format"]:
                    del sv["format"]
                if sv["type"] == "string":
                    del sv["format"]

                ret[table_key]["properties"][sk] = sv
            if request_body:
                ret[table_key]["required"] = required_array
        if not is_object:
            schema_name = None
            if table_key not in self.schemas:
                self.schemas[table_key] = ret[table_key]
                self.schema_counter[table_key] = 1
                schema_name = table_key
            else:
                schema_found = False
                for ks, vs in self.schemas.items():
                    if vs == ret[table_key]:
                        schema_name = ks
                        schema_found = True
                        break

                if not schema_found:
                    schema_name = table_key + "_" + str(self.schema_counter[table_key])
                    self.schema_counter[table_key] += 1
                    self.schemas[schema_name] = ret[table_key]

            return schema_name

        return ret

    def generate_mongo_object(self, object_data, request_body):
        ret = {"type": "object", "properties": {}}

        field_key = object_data["key"]
        field_name = object_data["sourceName"]

        field_key = field_key.split(".", 1)
        field_collection = field_key[0]
        field_ref = f"{field_key[0]}." if len(field_key) > 1 else field_name

        field_schema = self.schema_data[field_collection]
        field_data = field_schema

        for fr in field_ref.split("."):
            field_data = field_data[fr]

        field_type = field_data.get("ezapi_type")

        if request_body:
            ret["required"] = []

    def generate_object(self, object_data, request_body):
        MG = MongoGenerator(self.projectid, self.db, self.schema_data)

        ret = {"type": "object", "properties": {}}
        if request_body:
            ret["required"] = []

        for k, v in object_data["properties"].items():
            if request_body:
                    ret["required"].append(v["name"])
            if v["type"] == "ezapi_table":
                # dict_merge(ret["properties"], self.generate_table(v, is_parent_object=True))
                #is_array = v["isArray"]
                is_array = v.get("isArray", False)
                if is_array:
                    dict_merge(ret["properties"], self.generate_array_table(v, request_body))
                else:
                    dict_merge(ret["properties"], self.generate_table(v, request_body, is_object=True))

            elif v["type"] == "object" and v.get("paramType") == "documentField":
                mongo_obj = {}
                mongo_obj[k] = MG.generate_mongo_object(v)
                dict_merge(ret["properties"], mongo_obj)
            elif v["type"] == "object" and v.get("schemaName") == "global":
                param_obj = {}
                param_obj[k] = self.generate_object(v, request_body)
                dict_merge(ret["properties"], param_obj)
            elif v["type"] in ["string", "number", "integer", "array", "boolean", "oid", "date"] or (v["type"] == "object" and "schemaRef" in v):
                dict_merge(ret["properties"], self.generate_field(v))
            elif v["type"] == "arrayOfObjects":
                temp = {
                    v["name"]: {"type": "array", "items": self.generate_object(v["items"], request_body)},
                }
                dict_merge(ret["properties"], temp)
        return ret

    def generate_body(self, body_data, request_body):
        ret = {}
        if request_body:
            ret["required"] = []                               
        body_type = body_data.get("type")
        if body_type == "object":
            if "schemaRef" in body_data:
                ret = {"type": "object", "properties": self.generate_field(body_data)}
                if body_data["required"] and request_body:
                    ret["required"].append(body_data["name"])
            else:
                ret = self.generate_object(body_data, request_body)
        elif body_type == "ezapi_table":
            is_array = body_data.get("isArray")
            schema_name = self.generate_table(body_data, request_body)
            
            if is_array:
                ret = {"type" : "array", "items" : {}}
                ret["items"] = {"ezapi_ref": f"#/components/schemas/{schema_name}"}
            else:                                               
                # ret = {"type": "object", "properties": self.generate_table(body_data)}
                ret = {"ezapi_ref": f"#/components/schemas/{schema_name}"}
        elif body_type in ["string", "number", "integer" ,"array"]:
            ret = {"type": "object", "properties": self.generate_field(body_data)}
            if body_data["required"] and request_body:
                    ret["required"].append(body_data["name"])
        return ret

    def generate_path(self):
        res = {}

        for path in self.operations:
            endpoint = path["endpoint"]
            method = path["method"]
            request_data = path["requestData"]
            response_data = path["responseData"]

            if endpoint not in res:
                res[endpoint] = {}
            if method not in res[endpoint]:
                res[endpoint][method] = {}

            # tags, summary, description, operationId
            for k in PATH_DESCRIPTION_KEYS:
                if k in self.operations:
                    res[endpoint][method][k] = self.operations[k]

            res[endpoint][method]["operationId"] = path["operationId"]
            res[endpoint][method][PARAMETER_KEY] = []
            res[endpoint][method][RESPONSE_KEY] = {}

            res[endpoint][method][BODY_KEY] = {}

            for param_type, param_data in request_data.items():
                if param_type in PATH_PARAMETERS:
                    for param in param_data:
                        for k, v in param.items():
                            param_name = k
                            param_value = v

                            tmp = {
                                "name": param_name,
                                "in": param_type,
                                "required": param_value.get("required", True),
                                "schema": {
                                    "type": param_value["type"],
                                    "format": param_value.get("format", None),
                                },
                            }

                            if param_type == "path":
                                tmp["required"] = True

                            if not tmp["schema"]["format"]:
                                del tmp["schema"]["format"]

                            if tmp["schema"]["type"] == "string":
                                del tmp["schema"]["format"]

                            res[endpoint][method][PARAMETER_KEY].append(tmp)

                elif param_type in ("body", "formData") and param_data:
                    if param_type == "body":
                        body_data = param_data
                        tmp = {}

                        tmp["description"] = ""
                        tmp["content"] = {}
                        tmp["content"]["application/json"] = {}
                        tmp["content"]["application/json"][
                            "schema"
                        ] = self.generate_body(body_data, request_body = True)
                        res[endpoint][method][BODY_KEY] = tmp
                    elif param_type == "formData":
                        pass

            # responses
            res[endpoint][method]["responses"] = {}
            for resp in response_data:
                status = resp["status_code"]
                resp_dict = {}

                res[endpoint][method]["responses"][status] = {}
                for k in RESPONSE_KEYS:
                    if k == "content":
                        if k in resp:
                            resp_dict[k] = {}
                            resp_dict[k]["application-json"] = {}
                            resp_dict[k]["application-json"][
                                "schema"
                            ] = self.generate_body(resp[k], request_body = False)
                        else:
                            resp_dict[k] = {}
                    elif k == "description":
                        resp_dict[k] = resp[k]
                    elif k == "headers":
                        if resp[k] and len(resp[k]) > 0:
                            resp_dict[k] = {}

                            for h in resp[k]:
                                for hk, hv in h.items():

                                    resp_dict[k][hk] = {
                                        "description": hv.get("description"),
                                        "schema": {
                                            "type": hv["type"],
                                            "format": hv.get("format", None),
                                        },
                                    }

                                    if not resp_dict[k][hk]["schema"]["format"]:
                                        del resp_dict[k][hk]["schema"]["format"]
                                    if not resp_dict[k][hk]["description"]:
                                        del resp_dict[k][hk]["description"]
                                    if resp_dict[k][hk]["schema"]["type"] == "string":
                                        del resp_dict[k][hk]["schema"]["format"]

                res[endpoint][method]["responses"][status] = resp_dict

            if not res[endpoint][method][BODY_KEY]:
                del res[endpoint][method][BODY_KEY]

        self.paths = res

    def write_spec(self):
        project_name = self.project_data.get("projectName", "API Project")

        self.spec = {
            "openapi": "3.0.0",
            "info": {
                "title": project_name,
                "description": "EzAPI Generated Spec - OpenAPI 3.0",
                "version": "0.0.1",
            },
            "tags": [],
            "paths": self.paths,
            "components":{
                "schemas": self.schemas
            }
        }
        return self.spec


def generate_spec(project_data, projectid, db):
    try:
        SG = SpecGenerator(projectid, db)
        SG.project_data = project_data

        if not SG.operations:
            return {
                "success": False,
                "status": 404,
                "message": "operation data not found",
            }

        SG.generate_path()
        spec_data = SG.write_spec()

        spec_document = {"projectid": projectid, "data": spec_data}
        mongo.store_document(SPEC_COLLECTION, spec_document, db)

        return {"success": True, "status": 200, "message": "ok"}
    except Exception as e:
        print("Spec Generator Error - ", str(e))
        return {"success": False, "status": 500, "message": str(e)}

