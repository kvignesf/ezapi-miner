# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************

import os

from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin

from apiops.main import APIOPSModel

app = Flask(__name__)
CORS(app, support_credentials=True)


def run_apiops_model(filepath, filename, api_ops_id, dbname):
    apiops_obj = APIOPSModel(filepath, filename, api_ops_id, dbname)
    apiops_obj.get_db_instance()

    parser_result = apiops_obj.parse_swagger_specs()
    if 'success' in parser_result and parser_result['success']:

        scorer_result = apiops_obj.score_request_elements()
        if 'success' in scorer_result and scorer_result['success']:

            visualizer_result = apiops_obj.prepare_sankey_data()
            if 'success' in visualizer_result and visualizer_result['success']:

                tester_result = apiops_obj.generate_test_data()
                if 'success' in tester_result and tester_result['success']:

                    ret = {
                        'success': True,
                        'status': 200,
                        'message': 'ok',
                        'stage': 'tester',
                        'data': {
                            'api_ops_id': api_ops_id,
                            'test_count': tester_result['data']['testcases_count'],
                            'api_summary': parser_result['data']['api_summary']
                        }
                    }

                else:
                    ret = tester_result
                    ret['stage'] = 'visualizer'

            else:
                ret = visualizer_result
                ret['stage'] = 'scorer'

        else:
            ret = scorer_result
            ret['stage'] = 'parser'

    else:
        ret = parser_result
        ret['stage'] = 'not scored'

    apiops_obj.client.close()
    return ret


@app.route("/")
def home():
    return "Hello APIOPS"


@app.route("/apiops_model", methods=["POST"])
def apiops_model():
    f = request.files["file"]
    filename = f.filename

    dbname = str(request.form['dbname'])
    api_ops_id = str(request.form['api_ops_id'])

    if not os.path.exists('./uploads'):
        os.makedirs('./uploads')

    filepath = "./uploads/" + filename
    f.save(filepath)

    res = run_apiops_model(filepath, filename, api_ops_id, dbname)

    try:
        os.remove(filepath)
    except Exception as e:
        print("Error deleting Uploaded File")

    return jsonify(res)


if __name__ == "__main__":
    app.run(debug=True)
