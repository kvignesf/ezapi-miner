# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************

import json
import os

from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin

from api_designer.main import EzAPIModels, BaseEzapiModels

from werkzeug import Request, Response
from werkzeug import exceptions as exc
from decouple import config


def json_response(data, status=200):
    return Response(json.dumps(data, default=str), content_type="application/json", status=status)


def bad_request(data):
    raise exc.BadRequest(response=json_response(data, status=400))


def unprocessable_entity(data):
    raise exc.UnprocessableEntity(response=json_response(data, status=422))


def internal_server_error(data):
    raise exc.InternalServerError(response=json_response(data, status=500))


app = Flask(__name__)
CORS(app, support_credentials=True)


@app.route("/")
def home():
    return "Hello EzAPI"

@app.route("/keywordchecker", methods=["POST"])
def kw_checker():
    request_data = request.get_json()
    keywrd = str(request_data.get("keywrd", ""))
    dbtype = str(request_data.get("dbtype", ""))
    if keywrd and dbtype:
        model = BaseEzapiModels(keywrd)
        model.set_db_instance()
        ret = model.check_keywrd(dbtype)
        model.client.close()
    else:
        bad_request({"success": False, "message": "Some parameters are missing: keywrd, dbtype"})

    return json_response({"data": ret})


@app.route("/db_extractor", methods=["POST"])
def db_extractor():
    print("In DB Extractor")
    request_data = request.get_json()
    projectid = str(request_data.get("projectid", ""))
    dbtype = str(request_data.get("dbtype", ""))

    if projectid and dbtype:
        model = EzAPIModels(projectid)
        model.set_db_instance()
        ret = model.extract_sql_connect(request_data, dbtype)
        model.client.close()
    else:
        bad_request({"success": False, "message": "Some parameters are missing: projectid, dbtype"})

    return json_response({"data": ret})

@app.route("/matcher", methods=["POST"])
def mathcer_model():
    print("Matcher Received")
    request_data = request.get_json()
    projectid = str(request_data.get("projectid", ""))

    if projectid:
        model = EzAPIModels(projectid)
        model.set_db_instance()
        ret = model.matcher()
        model.client.close()

    else:
        bad_request({"success": False, "message": "Some parameters are missing"})

    return json_response(ret)


@app.route("/ddl_parser", methods=["POST"])
def ddl_parser_model():
    print("DDL Parser Received")
    ddl_file = request.files.getlist("ddl_file", None)
    projectid = str(request.form.get("projectid", ""))
    ddltype = str(request.form.get("dbtype", "mssql"))

    if not os.path.exists("./uploads"):
        os.makedirs("./uploads")

    if ddl_file and projectid:
        if len(ddl_file) != 1:
            ret = bad_request(
                {"success": False, "message": "Some parameters are missing"}
            )
        else:
            ddl_file = ddl_file[0]
            ddl_filename = ddl_file.filename
            ddl_path = "./uploads/" + ddl_filename
            ddl_file.save(ddl_path)

            model = EzAPIModels(projectid)
            model.set_db_instance()
            ret = model.parse_ddl_file(ddl_path, ddl_filename, ddltype)
            # model.client.close()
    else:
        ret = bad_request({"success": False, "message": "Some parameters are missing"})

    try:
        os.remove(ddl_path)
    except Exception as e:
        print("Error deleting Uploaded File")

    return json_response(ret, status=ret["status"])


@app.route("/raw_spec_parser", methods = ["POST"])
def raw_spec_parser_model():
    request_data = request.get_json()
    projectid = str(request_data.get("projectid", ""))
    if projectid:
        model = EzAPIModels(projectid)
        model.set_db_instance()
        # ret = model.sim_artefacts_generator()
        ret = model.raw_spec_parser()
        model.client.close()

    else:
        ret = bad_request({"success": False, "message": "Some parameters are missing"})

    return json_response(ret, status=ret["status"])


@app.route("/spec_parser", methods=["POST"])
def spec_parser_model():
    print("Spec Parser Received")
    spec_file = request.files.get("spec_file", None)
    projectid = str(request.form.get("projectid", ""))

    if not os.path.exists("./uploads"):
        os.makedirs("./uploads")

    if spec_file and projectid:
        spec_filename = spec_file.filename
        spec_path = "./uploads/" + spec_filename
        spec_file.save(spec_path)

        model = EzAPIModels(projectid)
        model.set_db_instance()
        ret = model.parse_spec_file(spec_path, spec_filename)
        model.client.close()
    else:
        ret = bad_request({"success": False, "message": "Some parameters are missing"})

    try:
        os.remove(spec_path)
    except Exception as e:
        print("Error deleting Uploaded File")

    return json_response(ret, status=ret["status"])


@app.route("/spec_generator", methods=["POST"])
def spec_generator_model():
    print("Spec Generator Received")
    request_data = request.get_json()
    projectid = str(request_data.get("projectid", ""))

    if projectid:
        model = EzAPIModels(projectid)
        model.set_db_instance()
        ret = model.spec_generator()
        model.client.close()

    else:
        ret = bad_request({"success": False, "message": "Some parameters are missing"})

    return json_response(ret, status=ret["status"])


@app.route("/artefacts", methods=["POST"])
def artefacts_model():
    print("Artefacts Received")
    request_data = request.get_json()
    projectid = str(request_data.get("projectid", ""))

    if projectid:
        model = EzAPIModels(projectid)
        model.set_db_instance()
        ret = model.artefacts_generator()
        model.client.close()

    else:
        ret = bad_request({"success": False, "message": "Some parameters are missing"})

    return json_response(ret, status=ret["status"])


@app.route("/simulation_artefacts", methods=["POST"])
def simulation_model():
    print("Sim Artefacts Received")
    request_data = request.get_json()
    projectid = str(request_data.get("projectid", ""))
    operationId = str(request_data.get("operationId", ""))

    if projectid:
        model = EzAPIModels(projectid)
        model.set_db_instance()
        ret = model.sim_artefacts_generator(operationId)
        model.client.close()

    else:
        ret = bad_request({"success": False, "message": "Some parameters are missing"})

    return json_response(ret, status=ret["status"])



@app.route("/sankey", methods=["POST"])
def sankey_model():
    print("Sankey Received")
    request_data = request.get_json()
    projectid = str(request_data.get("projectid", ""))

    if projectid:
        model = EzAPIModels(projectid)
        model.set_db_instance()
        ret = model.sankey_generator()
        model.client.close()

    else:
        ret = bad_request({"success": False, "message": "Some parameters are missing"})

    return json_response(ret, status=ret["status"])


@app.route("/codegen", methods=["POST"])
def codegen_model():
     print("Codegen Received")
     request_data = request.get_json()
     projectid = str(request_data.get("projectid", ""))

     if projectid:
         model = EzAPIModels(projectid)
         model.set_db_instance()
         ret = model.jdl_generator()
         model.client.close()
     else:
         ret = bad_request({"success": False, "message": "Some parameters are missing"})

     return json_response(ret, status=ret["status"])


@app.route("/db_ddl_parser", methods=["POST"])
def db_ddl_parser_model():
    print("DDL Parser Received")
    ddl_file = request.files.getlist("ddl_file", None)
    projectid = str(request.form.get("projectid", ""))
    dbType = str(request.form.get("dbtype", ""))

    if not os.path.exists("./uploads"):
        os.makedirs("./uploads")

    if ddl_file and projectid and dbType:
        if len(ddl_file) != 1:
            ret = bad_request(
                {"success": False, "message": "Some parameters are missing"}
            )
        else:
            ddl_file = ddl_file[0]
            ddl_filename = ddl_file.filename
            ddl_path = "./uploads/" + ddl_filename
            ddl_file.save(ddl_path)

            model = EzAPIModels(projectid)
            model.set_db_instance()
            ret = model.parse_db_ddl_file(ddl_path, ddl_filename, dbType)
            model.client.close()
    else:
        ret = bad_request({"success": False, "message": "Some parameters are missing"})

    try:
        os.remove(ddl_path)
    except Exception as e:
        print("Error deleting Uploaded File")

    return json_response(ret, status=ret["status"])

@app.route("/db_ddl_generator", methods=["POST"])
def db_ddl_generator_model():
    print("DDL Parser Received")
    #ddl_file = request.files.getlist("ddl_file", None)
    projectid = str(request.form.get("projectid", ""))
    server = str(request.form.get("server", ""))
    username = str(request.form.get("username", ""))
    password = str(request.form.get("password", ""))
    database = str(request.form.get("database", ""))
    #dbtype = str(request.form.get("dbtype", ""))

    if not os.path.exists("./uploads"):
        os.makedirs("./uploads")

    if server and username and password and database:
        if projectid:
            # ddl_file = ddl_file[0]
            # ddl_filename = ddl_file.filename
            # ddl_path = "./uploads/" + ddl_filename
            # ddl_file.save(ddl_path)
            #
            model = EzAPIModels(projectid)
            model.set_db_instance()
            ret = model.gen_db_ddl_file(server, username, password, database)
            model.client.close()
    else:
        ret = bad_request({"success": False, "message": "Some parameters are missing"})

    # try:
    #     os.remove(ddl_path)
    # except Exception as e:
    #     print("Error deleting Uploaded File")

    return json_response(ret, status=ret["status"])


@app.route("/artefacts2", methods=["POST"])
def run_artefacts2():
    print("Artefacts Received")
    request_data = request.get_json()
    projectid = str(request_data.get("projectid", ""))
    type = str(request_data.get("type", "functional")) # or performance

    if projectid:
        model = EzAPIModels(projectid)
        model.set_db_instance()
        ret = model.artefacts_generator2(type)
        model.client.close()
    else:
        ret = bad_request({"success": False, "message": "Some parameters are missing"})

    return json_response(ret, status=ret["status"])

@app.route("/update_tests", methods=["POST"])
def run_update_tests():
    print("Update Testcases Received")
    request_data = request.get_json()
    projectid = str(request_data.get("projectid", ""))

    if projectid:
        model = EzAPIModels(projectid)
        model.set_db_instance()
        ret = model.update_testdata()
        model.client.close()

    else:
        ret = bad_request({"success": False, "message": "Some parameters are missing"})

    return json_response(ret, status=ret["status"])

@app.route("/extract_sp", methods=["POST"])
def extract_stored_procs():
    print("Update Testcases Received")
    request_data = request.get_json()
    projectid = str(request_data.get("projectid", ""))
    dbtype = str(request_data.get("dbtype", ""))
    if projectid:
        model = EzAPIModels(projectid)
        model.set_db_instance()
        ret = model.extract_sp_data(request_data, dbtype)
        model.client.close()

    else:
        ret = bad_request({"success": False, "message": "Some parameters are missing"})

    return json_response(ret, status=ret["status"])

@app.route("/mongo_extractor", methods=["POST"])
def mongo_extractor():
    print("In DB Extractor")
    request_data = request.get_json()
    projectid = str(request_data.get("projectid", ""))
    dbtype = str(request_data.get("dbtype", ""))

    if projectid and dbtype:
        model = EzAPIModels(projectid)
        model.set_db_instance()
        ret = model.extract_nosql_connect(request_data, dbtype)
        model.client.close()
    else:
        bad_request({"success": False, "message": "Some parameters are missing: projectid, dbtype"})

    return json_response({"data": ret})

@app.route("/artefacts_mongo", methods=["POST"])
def run_artefacts_mongo():
    print("Artefacts Mongo Received")
    request_data = request.get_json()
    projectid = str(request_data.get("projectid", ""))
    type = str(request_data.get("type", "functional")) # or performance

    if projectid:
        model = EzAPIModels(projectid)
        model.set_db_instance()
        ret = model.artefacts_generator_mongo(type)
        model.client.close()

    else:
        ret = bad_request({"success": False, "message": "Some mongo parameters are missing"})

    return json_response(ret, status=ret["status"])


if __name__ == "__main__":
    app.run(debug=True, port=5000)
