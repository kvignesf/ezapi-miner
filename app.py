# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************

import json
import os

from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin

from api_designer.main import EzAPIModels

app = Flask(__name__)
CORS(app, support_credentials=True)


@app.route("/")
def home():
    return "Hello EzAPI"


@app.route("/specgen", methods=["POST"])
def specgen_model():
    print("Received")
    request_data = request.get_json()
    projectid = str(request_data.get("projectid", ""))

    if projectid:
        model = EzAPIModels(projectid)
        model.set_db_instance()
        ret = model.spec_generator()
        model.client.close()

    else:
        ret = {
            "status": 400,
            "success": False,
            "message": "Some parameters are missing",
        }

    return jsonify(ret)


@app.route("/matcher", methods=["POST"])
def mathcer_model():
    request_data = request.get_json()
    projectid = str(request_data.get("projectid", ""))

    if projectid:
        model = EzAPIModels(projectid)
        model.set_db_instance()
        ret = model.matcher()
        model.client.close()

    else:
        ret = {
            "status": 400,
            "success": False,
            "message": "Some parameters are missing",
        }

    return jsonify(ret)


@app.route("/ddl_parser", methods=["POST"])
def ddl_parser_model():
    ddl_file = request.files.getlist("ddl_file", None)
    projectid = str(request.form.get("projectid", ""))

    if not os.path.exists("./uploads"):
        os.makedirs("./uploads")

    if ddl_file and projectid:
        if len(ddl_file) != 1:
            ret = {
                "status": 400,
                "success": False,
                "message": "Only one DDL file supported",
            }
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
        ret = {
            "status": 400,
            "success": False,
            "message": "Some parameters are missing",
        }

    try:
        os.remove(ddl_path)
    except Exception as e:
        print("Error deleting Uploaded File")

    return jsonify(ret)


@app.route("/spec_parser", methods=["POST"])
def spec_parser_model():
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
        ret = {
            "status": 400,
            "success": False,
            "message": "Some parameters are missing",
        }

    try:
        os.remove(spec_path)
    except Exception as e:
        print("Error deleting Uploaded File")

    return jsonify(ret)


@app.route("/spec_generator", methods=["POST"])
def spec_generator_model():
    request_data = request.get_json()
    projectid = str(request_data.get("projectid", ""))

    if projectid:
        model = EzAPIModels(projectid)
        model.set_db_instance()
        ret = model.spec_generator()
        model.client.close()

    else:
        ret = {
            "status": 400,
            "success": False,
            "message": "Some parameters are missing",
        }

    return jsonify(ret)


@app.route("/artefacts", methods=["POST"])
def artefacts_model():
    request_data = request.get_json()
    projectid = str(request_data.get("projectid", ""))

    if projectid:
        model = EzAPIModels(projectid)
        model.set_db_instance()
        ret = model.artefacts_geerator()
        model.client.close()

    else:
        ret = {
            "status": 400,
            "success": False,
            "message": "Some parameters are missing",
        }

    return jsonify(ret)


@app.route("/sankey", methods=["POST"])
def sankey_model():
    request_data = request.get_json()
    projectid = str(request_data.get("projectid", ""))

    if projectid:
        model = EzAPIModels(projectid)
        model.set_db_instance()
        ret = model.sankey_generator()
        model.client.close()

    else:
        ret = {
            "status": 400,
            "success": False,
            "message": "Some parameters are missing",
        }

    return jsonify(ret)


if __name__ == "__main__":
    app.run(debug=True)
