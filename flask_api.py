from swagger_parser import parse_swagger_api
from param_functions import handle_param_functions
from visualizer import fetch_sankey_data

from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app, support_credentials=True)


@app.route("/")
def home():
    return "Hello APIOPS"


@app.route("/swagger_parser", methods=["POST"])
def swagger_parser():
    f = request.files["file"]

    filepath = "./files/inputfile.json"
    f.save(filepath)

    res = parse_swagger_api(filepath)
    return jsonify(res)


@app.route("/param_functions", methods=["POST"])
@cross_origin(supports_credentials=True)
def param_functions():
    request_data = request.get_json()
    api_ops_id = request_data.get("api_ops_id")
    res = handle_param_functions(api_ops_id)
    return jsonify(res)


@app.route("/visualizer", methods=["GET"])
def visualizer_function():
    request_data = dict(request.args)
    if 'api_ops_id' in request_data:
        api_ops_id = str(request_data['api_ops_id'])
    if 'tag' in request_data:
        tag = str(request_data['tag'])

    print(api_ops_id, tag)

    result = fetch_sankey_data(api_ops_id, tag)
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True)
