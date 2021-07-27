# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************

import json
import os

from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin

from api_designer.main import EzAPIModels

from werkzeug import Request, Response
from werkzeug import exceptions as exc


def json_response(data, status=200):
    return Response(json.dumps(data), content_type="application/json", status=status)


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

    return json_response(ret, status=ret["status"])


@app.route("/ddl_parser", methods=["POST"])
def ddl_parser_model():
    print("DDL Parser Received")
    ddl_file = request.files.getlist("ddl_file", None)
    projectid = str(request.form.get("projectid", ""))

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
            ret = model.parse_ddl_file(ddl_path, ddl_filename)
            model.client.close()
    else:
        ret = bad_request({"success": False, "message": "Some parameters are missing"})

    try:
        os.remove(ddl_path)
    except Exception as e:
        print("Error deleting Uploaded File")

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
        ret = model.artefacts_geerator()
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


if __name__ == "__main__":
    app.run(debug=True)
