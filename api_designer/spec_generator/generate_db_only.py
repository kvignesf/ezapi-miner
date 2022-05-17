# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************


from pprint import pprint
from api_designer import mongo

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

        self.spec = {}
        self.schemas_list = set()

        self.operations = self.db.operationdatas.find({"projectid": self.projectid})
        self.tables = self.db.tables.find({"projectid": self.projectid})
        self.operations = [x["data"] for x in self.operations]

        print("DB Data fetched")

    def generate_field(self, field_data):
        ret = {}
        name = field_data["name"]
        values = {
            "type": field_data["type"],
            "format": field_data.get("format", None),
            # "required": field_data.get("required", True), # no required field inside schema objects
        }
        if not values["format"]:
            del values["format"]
        ret[name] = values
        return ret

    def generate_table(self, table_data, is_object = False):   # convert it into a schema
        ret = {}
        # ret2 = {}
        table_key = table_data["name"]
        selected_columns = table_data.get("selectedColumns", [])

        if selected_columns:
            ret = {
                table_key: {"type": "object", "properties": {}},
            }
            for s in selected_columns:
                sk = s["name"]
                sv = {
                    "type": s["type"],
                    "format": s.get("format", None),
                    # "required": s.get("required", True), # no required field inside schema objects
                }
                if not sv["format"]:
                    del sv["format"]

                ret[table_key]["properties"][sk] = sv

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

    def generate_object(self, object_data):
        ret = {"type": "object", "properties": {}}

        for k, v in object_data["properties"].items():
            if v["type"] == "ezapi_table":
                # dict_merge(ret["properties"], self.generate_table(v, is_parent_object=True))
                dict_merge(ret["properties"], self.generate_table(v, is_object=True))
            elif v["type"] in ["string", "number", "integer"]:
                dict_merge(ret["properties"], self.generate_field(v))

        return ret

    def generate_body(self, body_data):
        ret = {}
        body_type = body_data.get("type")
        if body_type == "object":
            ret = self.generate_object(body_data)
        elif body_type == "ezapi_table":
            # ret = {"type": "object", "properties": self.generate_table(body_data)}
            schema_name = self.generate_table(body_data)
            ret = {"$ref": f"#/components/schemas/{schema_name}"}
        elif body_type in ["string", "number", "integer"]:
            ret = {"type": "object", "properties": self.generate_field(body_data)}
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
                        ] = self.generate_body(body_data)
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
                            ] = self.generate_body(resp[k])
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

