# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************

import pathlib
from api_designer.codegen.entity_init import extract_entity_tables
from pathlib import Path
import pymongo
import re
import subprocess


def convert_to_camel_case(s):
    res = re.split(r"[^a-zA-Z0-9]", s)
    res = [x.lower() for x in res]
    res = "".join(x.title() for x in res)
    return res


# def get_db_connection(dbname="ezapi", host="localhost", port=27017):
#     # client = pymongo.MongoClient(host, port)
#     client = pymongo.MongoClient(
#         "mongodb://root:JRVvuh9D5V0IZxCW@34.66.45.162:27017/?authSource=admin",
#         Connect=False,
#     )
#     db = client[dbname]
#     return (client, db)


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

        self.infile = "./template.jdl"
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
        cmd2 = "jhipster jdl " + self.outfile
        cmd = cmd1 + "; " + cmd2

        subprocess.run(cmd, shell=True)


def generate_jdl_file(projectid, db):
    project_data = db.projects.find_one({"projectId": projectid})
    operation_data = db.operaationdatas.find({"projectid": projectid})
    table_data = db.tables.find({"projectid": projectid})

    project_type = project_data.get("projectType", None)

    gt = GenerateTemplate()
    if project_type not in ("db", "both"):
        return {
            "success": False,
            "status": 500,
            "message": "Project type not supported for code generation",
        }

    Path("/mnt/codegen/" + projectid).mkdir(parents=True, exist_ok=True)

    gt.project_data = project_data
    gt.project_type = project_type
    gt.operation_data = list(operation_data)
    gt.table_data = list(table_data)
    gt.outfile = "/mnt/codegen/" + projectid + "/" + projectid + ".jdl"

    if project_type == "both":
        schemas_data = db.components.find_one({"projectid": projectid})
        schemas_data = schemas_data.get("data").get("schemas", {})
        gt.schemas_data = schemas_data

        matcher_data = db.matcher.find({"projectid": projectid})
        gt.matcher_data = list(matcher_data)

    gt.generate_jdl()
    gt.generate_code()

    db.projects.update_one({"projectId": projectid}, {"$set": {"codegen": True}})

    return {"success": True, "status": 200, "message": "ok"}
