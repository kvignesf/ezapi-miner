# from swagger_parser import parse_swagger_api
# from param_functions import handle_param_functions
# from visualizer import fetch_sankey_data
from apifunctions import parse_swagger_openapi, get_sankey_data, generate_test_cases, run_apiops_model

from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app, support_credentials=True)


@app.route("/")
def home():
    return "Hello APIOPS"


@app.route("/apiops_parser", methods=["POST"])
def apiops_parser():
    f = request.files["file"]
    filename = f.filename

    filepath = "./files/" + filename
    f.save(filepath)

    res = parse_swagger_openapi(filepath, filename)
    return jsonify(res)


@app.route("/visualizer", methods=["GET"])
def visualizer():
    api_ops_id = str(request.args.get('api_ops_id'))

    if not api_ops_id:
        return jsonify({"success": false, "status": 400, "message": "apiops id is missing"})

    result = get_sankey_data(api_ops_id)
    return jsonify(result)


@app.route("/generate_test", methods=["POST"])
def generate_test():
    api_ops_id = str(request.json.get('api_ops_id'))
    print(api_ops_id)

    if not api_ops_id:
        return jsonify({"success": false, "status": 400, "message": "apiops id is missing"})

    result = generate_test_cases(api_ops_id)
    return jsonify(result)


@app.route("/apiops_model", methods=["POST"])
def apiops_model():
    f = request.files["file"]
    filename = f.filename

    dbname = str(request.form['dbname'])
    print(dbname, filename)

    filepath = "./files/" + filename
    f.save(filepath)

    res = run_apiops_model(filepath, filename, dbname)
    return jsonify(res)


if __name__ == "__main__":
    app.run(debug=True)
