# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************


from api_designer import mongo
from api_designer.spec_generator import generate_db_only
from pprint import pprint

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


class SchemaDeref:
    def __init__(self, schemas):
        self.schemas = schemas  # All Schema
        self.node_schemas = []

    def deref_array(self, param_array):
        assert param_array["type"] == "array"

        array_items = param_array.get("items")
        array_items_type = array_items.get("type")

        if "ezapi_ref" in array_items:
            ref_schema = array_items["ezapi_ref"].split("/")[-1]
            self.node_schemas.append(ref_schema)

            ref_schema = self.schemas[ref_schema]
            self.deref_schema(ref_schema)

        else:
            if array_items_type == "array":
                self.deref_array(array_items)

            elif array_items_type == "object":
                self.deref_object(array_items)

    def deref_object(self, param_object):
        assert param_object["type"] == "object"

        if "properties" in param_object:  # only additionalProperties present
            for key, value in param_object["properties"].items():
                value_type = value.get("type")

                if "ezapi_ref" in value:
                    ref_schema = value["ezapi_ref"].split("/")[-1]
                    self.node_schemas.append(ref_schema)

                    ref_schema = self.schemas[ref_schema]
                    self.deref_schema(ref_schema)

                else:

                    if value_type == "object":
                        self.deref_object(value)
                    elif value_type == "array":
                        self.deref_array(value)

    def deref_schema(self, param_schema):
        if "allOf" in param_schema:
            all_schemas = param_schema["allOf"]

            for s in all_schemas:
                self.deref_schema(s)

        else:
            st = param_schema.get("type")
            if "ezapi_ref" in param_schema:
                ref_schema = param_schema["ezapi_ref"].split("/")[-1]
                self.node_schemas.append(ref_schema)

                ref_schema = self.schemas[ref_schema]
                self.deref_schema(ref_schema)

            elif st == "object":
                self.deref_object(param_schema)

            elif st == "array":
                self.deref_array(param_schema)

        return self.node_schemas


# required - should be an array of property names required within an object schema
def filter_content_fields(content):
    if not content:
        return content

    content_type = content["type"]
    if content_type in ["string", "number", "integer", "boolean"]:
        content = {
            k: v
            for k, v in content.items()
            if k in PARAMETER_SCHEMA_KEYS + ["description"] and v
        }
    elif content_type == "object":
        for k, v in content["properties"].items():
            content["properties"][k] = filter_content_fields(v)
    elif content_type == "ezapi_ref":
        content = {
            k: v for k, v in content.items() if k in ["description", "ezapi_ref"] and v
        }
    return content


class SpecGenerator:
    def __init__(self, projectid, db):
        self.projectid = projectid
        self.db = db

        self.spec_orig = db.raw_spec.find_one({"projectid": projectid})
        self.path_orig = db.operationdatas.find({"projectid": projectid})
        self.component_orig = db.components.find_one({"projectid": projectid})

        self.path_orig = [x["data"] for x in self.path_orig]
        self.spec_orig = self.spec_orig["data"]
        self.component_orig = self.component_orig["data"]

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

        self.spec = {}
        self.schemas_list = set()

    # Iterate through generated schemas and find all the schemas required
    def generate_schemas(self):
        schemas_orig = self.component_orig["schemas"]
        sd = SchemaDeref(schemas_orig)

        for sk, sv in self.responses.items():
            if (
                "content" in sv
                and "application-json" in sv["content"]
                and "schema" in sv["content"]["application-json"]
            ):
                content_schema = sv["content"]["application-json"]["schema"]
                ret = sd.deref_schema(content_schema)
                self.schemas_list.update(ret)

        for sk, sv in self.schemas.items():
            ret = sd.deref_schema(sv)
            self.schemas_list.update(ret)

        for t in self.schemas_list:
            if t not in self.schemas:
                self.schemas[t] = schemas_orig[t]

    def generate_path(self):
        res = {}
        for path in self.path_orig:

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
                if k in self.path_orig:
                    res[endpoint][method][k] = self.path_orig[k]

            res[endpoint][method]["operationId"] = path["operationId"]
            res[endpoint][method][PARAMETER_KEY] = []
            res[endpoint][method][RESPONSE_KEY] = {}
            res[endpoint][method][BODY_KEY] = {}

            for param_type, param_data in request_data.items():

                # parameters - path, query, header, cookie
                if param_type in PATH_PARAMETERS:
                    for param in param_data:  # array
                        for k, v in param.items():  # Only one item
                            param_name = k
                            param_value = v

                            tmp = {"name": param_name, "in": param_type, "schema": {}}

                            for x in PARAMETER_KEYS:
                                if x in param_value:
                                    x_val = param_value.get(x)
                                    if x_val:
                                        tmp[x] = x_val

                            for x in PARAMETER_SCHEMA_KEYS:
                                if x in param_value:
                                    x_val = param_value.get(x)
                                    if x_val:
                                        tmp["schema"][x] = x_val

                            if param_type == "header":  # Both the places
                                if (
                                    param_name not in self.headers
                                ):  # todo: Also check value data

                                    copy_tmp = tmp.copy()
                                    del copy_tmp["in"]
                                    del copy_tmp["name"]
                                    self.headers[param_name] = copy_tmp

                            if param_type == "path":
                                tmp["required"] = True
                                # self.parameters[param_name] = tmp
                            else:
                                if param_name not in self.parameters:
                                    self.parameters[param_name] = tmp

                            if param_type == "path":
                                if "parameters" not in res[endpoint]:
                                    res[endpoint]["parameters"] = []

                                # tmp = {
                                #     "ezapi_ref": "#/components/parameters/" + param_name
                                # }

                                is_pp_exist = False
                                for pp in res[endpoint]["parameters"]:
                                    if pp == tmp or tmp["name"] == pp["name"]:
                                        is_pp_exist = True

                                if not is_pp_exist:
                                    res[endpoint]["parameters"].append(tmp)

                            # elif param_type == "header":
                            #     res[endpoint][method][PARAMETER_KEY].append(
                            #         {"ezapi_ref": "#/components/headers/" + param_name}
                            #     )

                            else:
                                res[endpoint][method][PARAMETER_KEY].append(
                                    {
                                        "ezapi_ref": "#/components/parameters/"
                                        + param_name
                                    }
                                )

                # body, formaData
                elif param_type in ("body", "formData") and param_data:
                    if param_type == "body":
                        body_data = param_data

                        tmp = {}
                        tmp_schema = {}

                        tmp["description"] = ""
                        tmp["content"] = {}
                        tmp["content"]["application/json"] = {}
                        tmp["content"]["application/json"]["schema"] = {}
                        tmp["content"]["application/json"]["schema"][
                            "ezapi_ref"
                        ] = "Some ref"

                        if "ezapi_ref" in body_data:
                            tmp_ref = body_data["ezapi_ref"]
                            tmp["content"]["application/json"]["schema"][
                                "ezapi_ref"
                            ] = body_data["ezapi_ref"]

                            if "schemas" in tmp_ref:
                                tmp_schema = tmp_ref.split("/")[-1]
                                self.schemas[tmp_schema] = self.component_orig[
                                    "schemas"
                                ][tmp_schema]

                            request_body_name = tmp_schema
                            res[endpoint][method][BODY_KEY] = {
                                "ezapi_ref": "#/components/requestBodies/"
                                + request_body_name
                            }

                            if request_body_name not in self.requestBodies:
                                self.requestBodies[request_body_name] = tmp

                        else:
                            increase_schema_count = True
                            increase_request_count = True

                            generated_request_name = "Request" + str(
                                self.generated_request_count + 1
                            )
                            generated_schema_name = "Schema" + str(
                                self.generated_schema_count + 1
                            )

                            # check if body_data is array of some other schema
                            if (
                                body_data.get("type") == "array"
                                and "items" in body_data
                                and "ezapi_ref" in body_data["items"]
                            ):
                                child_schema = body_data["items"]["ezapi_ref"].split(
                                    "/"
                                )[-1]
                                generated_schema_name = child_schema + "_list"
                                increase_schema_count = False

                            # Check if schema already exist
                            is_schema_exist = False
                            for k, v in self.schemas.items():
                                if v == body_data:
                                    is_schema_exist = True
                                    generated_schema_name = k
                                    increase_schema_count = False
                                    break

                            if not is_schema_exist:
                                self.schemas[
                                    generated_schema_name
                                ] = SpecGenerator.prepare_body_data(body_data)

                            tmp["content"]["application/json"]["schema"][
                                "ezapi_ref"
                            ] = ("#/components/schemas/" + generated_schema_name)

                            is_request_exist = False
                            for k, v in self.requestBodies.items():
                                if v == tmp:
                                    is_request_exist = True
                                    generated_request_name = k
                                    increase_request_count = False

                            if not is_request_exist:
                                self.requestBodies[generated_request_name] = tmp

                            res[endpoint][method][BODY_KEY] = {
                                "ezapi_ref": "#/components/requestBodies/"
                                + generated_request_name
                            }

                            self.generated_request_count += increase_request_count
                            self.generated_schema_count += increase_schema_count

                    elif param_type == "formData":
                        pass

            # responses
            res[endpoint][method]["responses"] = {}
            for resp in response_data:
                status = resp["status_code"]
                resp_dict = {}

                res[endpoint][method]["responses"][status] = {}

                is_description_only = True
                for k in RESPONSE_KEYS:
                    if k in resp and resp[k] and k != "description":
                        is_description_only = False

                if is_description_only:
                    res[endpoint][method]["responses"][status]["description"] = resp[
                        "description"
                    ]

                else:
                    for k in RESPONSE_KEYS:
                        if k in resp and resp[k]:
                            if k == "content":
                                resp_dict[k] = {}
                                resp_dict[k]["application-json"] = {}
                                # resp_dict[k]["application-json"]["schema"] = resp[k]
                                resp_dict[k]["application-json"][
                                    "schema"
                                ] = SpecGenerator.filter_content_fields(resp[k])
                            elif k == "headers":
                                resp_dict[k] = {}
                                for _header in resp["headers"]:
                                    for _header_key, _header_data in _header.items():
                                        resp_dict[k][_header_key] = {
                                            "schema": SpecGenerator.filter_content_fields(
                                                _header_data
                                            )
                                        }
                            else:
                                resp_dict[k] = resp[k]

                    is_exist = False
                    matched = None

                    for rk, rd in self.responses.items():
                        if rd == resp_dict:  # already exist
                            is_exist = True
                            matched = rk

                    if not is_exist:
                        generated_response_name = "Response" + str(
                            self.generated_response_count + 1
                        )
                        self.generated_response_count += 1

                        self.responses[generated_response_name] = resp_dict
                        matched = generated_response_name

                    res[endpoint][method]["responses"][status] = {
                        "ezapi_ref": "#/components/responses/" + matched
                    }

            if not res[endpoint][method][BODY_KEY]:
                del res[endpoint][method][BODY_KEY]
        self.paths = res

    @staticmethod
    def prepare_body_data(body_data):
        ret = {}

        transform_body_data = {
            k: v
            for k, v in body_data.items()
            if k in PARAMETER_SCHEMA_KEYS + ["description", "required"] and v
        }

        if (
            "type" in body_data
            and body_data["type"] in ["string", "number", "integer", "boolean"]
            and "name" in body_data
        ):
            ret = {
                "type": "object",
                "properties": {body_data["name"]: transform_body_data},
            }
        else:
            ret = transform_body_data

        return ret

    # required - should be an array of property names required within an object schema
    @staticmethod
    def filter_content_fields(content):
        if not content:
            return content

        content_type = content["type"]
        if content_type in ["string", "number", "integer", "boolean"]:
            content = {
                k: v
                for k, v in content.items()
                if k in PARAMETER_SCHEMA_KEYS + ["description"] and v
            }
            return content
        elif content_type == "object":
            for k, v in content["properties"].items():
                content["properties"][k] = filter_content_fields(v)
            return content
        elif content_type == "ezapi_ref":
            content = {
                k: v
                for k, v in content.items()
                if k in ["description", "ezapi_ref"] and v
            }
            return content

    def filter_response_content(self):
        pass

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
            "components": {
                "schemas": self.schemas,
                "parameters": self.parameters,
                "headers": self.headers,
                "requestBodies": self.requestBodies,
                "responses": self.responses,
            },
        }
        return self.spec


def generate_spec(projectid, db):
    db.genspec.remove({"projectid": projectid})

    project_data = db.projects.find_one({"projectId": projectid})
    project_type = project_data.get("projectType", None)

    if not project_data or not project_type:
        return {
            "success": False,
            "status": 404,
            "message": "project data or project type not found",
        }

    if project_type == "db" or project_type == "noinput" or project_type == "aggregate":
        return generate_db_only.generate_spec(project_data, projectid, db)

    try:
        SG = SpecGenerator(projectid, db)
        SG.project_data = project_data

        if not SG.path_orig:
            return {
                "success": False,
                "status": 404,
                "message": "operation data not found",
            }

        SG.generate_path()
        SG.generate_schemas()
        spec_data = SG.write_spec()

        spec_document = {"projectid": projectid, "data": spec_data}
        mongo.store_document(SPEC_COLLECTION, spec_document, db)

        return {"success": True, "status": 200, "message": "ok"}
    except Exception as e:
        print("Spec Generator Error - ", str(e))
        return {"success": False, "status": 500, "message": str(e)}
