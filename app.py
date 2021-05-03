# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************

import os

from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin

from apiops.main import APIOPSModel
from api_designer.main import APIDesignModel

app = Flask(__name__)
CORS(app, support_credentials=True)


def run_api_design_model(
    spec_file, spec_filename, ddl_file, ddl_filename, api_design_id
):
    api_design_obj = APIDesignModel(
        spec_file, spec_filename, ddl_file, ddl_filename, api_design_id
    )
    api_design_obj.get_db_instance()

    spec_parser_result = api_design_obj.parse_spec_file()
    if "success" in spec_parser_result and spec_parser_result["success"]:
        ret = {"success": True, "status": 200, "message": "ok"}

        ddl_parser_result = api_design_obj.parse_ddl_file()
        if "success" in ddl_parser_result and ddl_parser_result["success"]:
            # ret = {"success": True, "status": 200, "message": "ok"}

            matcher_result = api_design_obj.matcher()
            if "success" in matcher_result and matcher_result["success"]:

                ret = {"success": True, "status": 200, "message": "ok"}

            else:
                ret = matcher_result
                ret["stage"] = "matching incompleted"

        else:
            ret = ddl_parser_result
            ret["stage"] = "ddl not parsed"

    else:
        ret = spec_parser_result
        ret["stage"] = "spec not parsed"

    api_design_obj.client.close()
    return ret


def run_apiops_model(filepath, filename, api_ops_id, dbname):
    apiops_obj = APIOPSModel(filepath, filename, api_ops_id, dbname)
    apiops_obj.get_db_instance()

    parser_result = apiops_obj.parse_swagger_specs()
    if "success" in parser_result and parser_result["success"]:

        scorer_result = apiops_obj.score_request_elements()
        if "success" in scorer_result and scorer_result["success"]:

            visualizer_result = apiops_obj.prepare_sankey_data()
            if "success" in visualizer_result and visualizer_result["success"]:

                tester_result = apiops_obj.generate_test_data()
                if "success" in tester_result and tester_result["success"]:

                    ret = {
                        "success": True,
                        "status": 200,
                        "message": "ok",
                        "stage": "tester",
                        "data": {
                            "api_ops_id": api_ops_id,
                            "test_count": tester_result["data"]["testcases_count"],
                            "api_summary": parser_result["data"]["api_summary"],
                        },
                    }

                else:
                    ret = tester_result
                    ret["stage"] = "visualizer"

            else:
                ret = visualizer_result
                ret["stage"] = "scorer"

        else:
            ret = scorer_result
            ret["stage"] = "parser"

    else:
        ret = parser_result
        ret["stage"] = "not scored"

    apiops_obj.client.close()
    return ret


@app.route("/")
def home():
    return "Hello APIOPS"


@app.route("/apiops_model", methods=["POST"])
def apiops_model():
    f = request.files["file"]
    filename = f.filename

    dbname = str(request.form["dbname"])
    api_ops_id = str(request.form["api_ops_id"])

    if not os.path.exists("./uploads"):
        os.makedirs("./uploads")

    filepath = "./uploads/" + filename
    f.save(filepath)

    res = run_apiops_model(filepath, filename, api_ops_id, dbname)

    try:
        os.remove(filepath)
    except Exception as e:
        print("Error deleting Uploaded File")

    return jsonify(res)


@app.route("/apidesign_model", methods=["POST"])
def apidesign_model():
    spec_file = request.files["spec_file"]
    ddl_file = request.files.getlist("ddl_file")
    api_design_id = str(request.form["api_design_id"])

    if len(ddl_file) != 1:
        return jsonify(
            {"success": False, "status": 500, "message": "Only one DDL file supported"}
        )

    spec_filename = spec_file.filename
    ddl_file = ddl_file[0]
    ddl_filename = ddl_file.filename

    if not os.path.exists("./uploads"):
        os.makedirs("./uploads")

    spec_path = "./uploads/" + spec_filename
    ddl_path = "./uploads/" + ddl_filename

    spec_file.save(spec_path)
    ddl_file.save(ddl_path)

    res = run_api_design_model(
        spec_path, spec_filename, ddl_path, ddl_filename, api_design_id
    )

    try:
        os.remove(spec_path)
        os.remove(ddl_path)
    except Exception as e:
        print("Error deleting Uploaded File")

    return jsonify(res)


if __name__ == "__main__":
    app.run(debug=True)
