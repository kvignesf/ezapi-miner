# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************

from pprint import pprint

from pathlib import Path
import json, re, shutil, subprocess
import requests

from api_designer.codegen.entity_init import extract_entity_tables
from api_designer.utils.schema_manager import SchemaDeref

# POJO_URL = "http://test-1-python.ezapi.ai:8098/gendtopojos"
POJO_URL = "http://localhost:8098/gendtopojos"


def convert_to_camel_case(s):
    res = re.split(r"[^a-zA-Z0-9]", s)
    res = [x.lower() for x in res]
    res = "".join(x.title() for x in res)
    return res


DIRECT_KEYS = set(["type", "format", "enum"])


class GenerateTemplate:
    def __init__(self, infile="./template.jdl"):
        self.config_data = {
            "baseName": "baseName",
            "packageName": "com.ezapi.api",
            "enableTranslation": "false",
            "languages": "[]",
            "applicationType": "microservice",
            "authenticationType": "jwt",
            "databaseType": "sql",
            "prodDatabaseType": "mssql",
            "devDatabaseType": "mssql",
            "skipClient": "true",
            "testFrameworks": "[]",
            "serviceDiscoveryType": "no",
            "serverPort": "8081",
            "reactive": "false",
            "buildTool": "maven",
        }

        self.infile = infile
        self.outfile = None
        self.project_data = None
        self.project_type = None
        self.schemas_data = None
        self.matcher_data = None
        self.table_data = None
        self.entity_data = None

    def set_field_value(self, field_name, field_value):
        if field_name in self.config_data:
            self.config_data[field_name] = field_value

    def generate_config_data(self):
        project_name = self.project_data.get("projectName", "API Project")
        project_name = convert_to_camel_case(project_name)
        self.set_field_value("baseName", project_name)

    def generate_entity_data(self):
        self.entity_data = extract_entity_tables(
            self.project_type,
            self.operation_data,
            self.schemas_data,
            self.matcher_data,
            self.table_data,
        )

    def generate_relationship_data(self):
        pass

    def generate_file(self):
        infile = open(self.infile, "r+")
        outfile = open(self.outfile, "w")
        filedata = infile.readlines()

        # config data
        for line in filedata:
            config_line = re.findall(r"\<.*?\>", line)
            str1 = None
            str2 = None

            if config_line:
                config_line = config_line[0]
                str1 = config_line
                config_line = config_line.strip("<>")

                if config_line in self.config_data:
                    config_line = self.config_data[config_line]
                    str2 = config_line

            if str1 and str2:
                line = line.replace(str1, str2)

            outfile.write(line)

        outfile.write("\n\n")

        # entity data
        if self.entity_data:
            for k, v in self.entity_data.items():
                entity_name = k
                entity_custom_name = v.get("custom_name", k)
                entity_definition_line = (
                    "entity " + entity_custom_name + "(" + entity_name + "){\n"
                )
                outfile.write(entity_definition_line)

                for i, (k2, v2) in enumerate(v["columns"].items()):
                    entity_value_name = k2
                    entity_value_custom_name = v2.get("custom_name", k2)
                    entity_value_type = v2.get("jdl_type", "string")
                    is_primary = v2.get("key", False)

                    entity_value_line = (
                        "\t"
                        + ("@Id " if is_primary else "")
                        + entity_value_custom_name
                        # + "("
                        # + entity_value_name
                        # + ") "
                        + " "
                        + entity_value_type
                    )

                    entity_value_line += "\n" if (i + 1 == len(v["columns"])) else ",\n"
                    outfile.write(entity_value_line)

                entity_endline = "}\n\n"
                outfile.write(entity_endline)

        outfile.write("\n")

        # relationship data

        # footer data
        outfile.write("service all with serviceImpl\n\n")
        outfile.write("dto * with mapstruct")

    def generate_jdl(self):
        self.generate_config_data()
        self.generate_entity_data()
        self.generate_relationship_data()
        self.generate_file()

    def generate_code(self):
        cmd1 = "cd " + self.outfile.rsplit("/", 1)[0]
        cmd2 = "jhipster jdl --force " + self.outfile
        cmd = cmd1 + "; " + cmd2

        subprocess.run(cmd, shell=True)


class GenerateSchemas:
    def __init__(self, operation_data, project_data, schemas=None):
        self.operation_data = operation_data
        self.project_data = project_data
        self.project_type = project_data.get("project_type")
        self.schemas = schemas

    def get_schema_data(self, schema_body):
        SD = SchemaDeref(self.schemas)
        return SD.deref_schema(schema_body)

    def get_field_data(self, data):
        return {x: data[x] for x in DIRECT_KEYS if x in data}

    def get_table_data(self, data):
        ret = {}
        selected_columns = data.get("selectedColumns", [])
        for s in selected_columns:
            sk = s["name"]
            sv = self.get_field_data(s)
            ret[sk] = sv

        return ret

    def get_table_objects(self, data):
        ret = {}

        if "properties" in data:
            for k, v in data["properties"].items():
                vtype = v.get("type")
                ret[k] = {}

                if v["type"] == "ezapi_table":
                    ret[k] = self.get_table_data(v)
                elif v["type"] in ["string", "number", "integer"]:
                    ret[k] = self.get_field_data(v)

        return ret

    def get_table_body(self, table_body):
        ret = {}
        body_type = table_body.get("type")

        if body_type == "object":
            ret = self.get_table_objects(table_body)
        elif body_type == "ezapi_table":
            ret = self.get_table_data(table_body)
        elif body_type in ["string", "number", "integer"]:
            ret = self.get_field_data(table_body)

        return ret

    def get_operation_schema(self):
        pojo_schemas = []
        tmp = 0

        for od in self.operation_data:
            path_data = od["data"]
            tmp += 1
            request_body = path_data["requestData"].get("body", {})
            response_data = path_data["responseData"]
            endpoint = path_data["endpoint"]

            if request_body:
                tmp_dict = {
                    "projectid": self.project_data["projectId"],
                    "name": path_data["method"]
                    + "Request"
                    + str(convert_to_camel_case(endpoint)),
                }
                tmp_dict["path"] = (
                    self.get_schema_data(request_body)
                    if self.project_type == "both"
                    else self.get_table_body(request_body)
                )
                pojo_schemas.append(tmp_dict)

            for resp in response_data:
                response_body = resp.get("content", {})
                if resp["status_code"][0] == "2" and response_body:  # 2xx
                    tmp_dict = {
                        "projectid": self.project_data["projectId"],
                        "name": path_data["method"]
                        + "Response"
                        + str(convert_to_camel_case(endpoint)),
                    }
                    tmp_dict["path"] = (
                        self.get_schema_data(response_body)
                        if self.project_type == "both"
                        else self.get_table_body(response_body)
                    )
                    pojo_schemas.append(tmp_dict)

        try:
            for ps in pojo_schemas:
                headers = {
                    "Content-type": "application/json",
                    "Accept": "application/json",
                }
                requests.post(POJO_URL, headers=headers, data=json.dumps(ps))
        except Exception as e:
            return False, "Unable to Generate POJO Files, " + str(e)

        return True, "Ok"


def generate_jdl_file(projectid, db):
    project_data = db.projects.find_one({"projectId": projectid})
    operation_data = db.operationdatas.find({"projectid": projectid})
    operation_data = list(operation_data)
    table_data = db.tables.find({"projectid": projectid})

    project_type = project_data.get("projectType", None)

    gt = GenerateTemplate()
    if project_type not in ("db", "both"):
        return {
            "success": False,
            "status": 500,
            "message": "Project type not supported for code generation",
        }

    dirpath = Path("/Users/shbham/mnt/codegen/" + projectid)
    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)

    Path("/Users/shbham/mnt/codegen/" + projectid).mkdir(parents=True, exist_ok=True)

    gt.project_data = project_data
    gt.project_type = project_type
    gt.operation_data = operation_data
    gt.table_data = list(table_data)
    gt.outfile = "/Users/shbham/mnt/codegen/" + projectid + "/" + projectid + ".jdl"

    schemas_data = None
    if project_type == "both":
        schemas_data = db.components.find_one({"projectid": projectid})
        schemas_data = schemas_data.get("data").get("schemas", {})
        gt.schemas_data = schemas_data

        matcher_data = db.matcher.find({"projectid": projectid})
        gt.matcher_data = list(matcher_data)

    gt.generate_jdl()
    gt.generate_code()

    # Remove node_modules
    node_modules_path = Path("/Users/shbham/mnt/codegen/" + projectid + "/node_modules")
    if node_modules_path.exists() and node_modules_path.is_dir():
        shutil.rmtree(node_modules_path)

    GS = GenerateSchemas(operation_data, project_data, schemas_data)
    ret, message = GS.get_operation_schema()

    if not ret:
        return {"success": False, "status": 500, "message": message}

    db.projects.update_one({"projectId": projectid}, {"$set": {"codegen": True}})

    return {"success": True, "status": 200, "message": "ok"}
