from swagger_parser import parse_swagger_api
from param_functions import handle_param_functions

from flask import Flask, request, jsonify

app = Flask(__name__)


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
def param_functions():
    request_data = request.get_json()
    api_ops_id = request_data.get("api_ops_id")
    res = handle_param_functions(api_ops_id)
    return jsonify(res)


if __name__ == "__main__":
    app.run(debug=True)
