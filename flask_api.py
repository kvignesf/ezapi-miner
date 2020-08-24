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
    print("In swagger parser")
    print(request.files)
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
    api_ops_id = str(request.args.get('api_ops_id'))
    tags = request.args.getlist('tag', None)

    if not api_ops_id:
        return jsonify({"success": false, "status": 500, "message": "api ops id is missing"})

    result = fetch_sankey_data(api_ops_id, tags)
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True)
