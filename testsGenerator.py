import db_manager
from pprint import pprint

from payload import get_request_data, get_response_data

db = db_manager.get_db_connection()

MAX_ITER = 10


def get_all_keys(d, res=[], prefix=""):
    for k, v in d.items():
        if isinstance(v, dict):
            res.append(prefix + "-" + k)
            get_all_keys(v, res, prefix + "-" + k)
        else:
            res.append(prefix + "-" + k)
    return res


def generate(api_ops_id):
    all_paths = db.paths.find({"api_ops_id": api_ops_id})
    for path in all_paths:
        endpoint = path['path']
        methods = path['allowed_method']

        for m in methods:
            print("\n--------------------------------------\n")
            print(endpoint, m, "==>")

            request_data = db.requests.find_one(
                {"api_ops_id": api_ops_id, "path": endpoint, "method": m})
            response_data = db.responses.find_one(
                {"api_ops_id": api_ops_id, "path": endpoint, "method": m})

            payload_response = get_response_data(response_data)
            print("\n==> payload response")
            pprint(payload_response)

            testcases = 0

            for resp in payload_response:
                if resp['status'] == '200':
                    ALL_REQUEST_KEYS_SET = set()

                    for iter in range(MAX_ITER):
                        payload_request = get_request_data(request_data)
                        request_keys = get_all_keys(
                            payload_request, res=[], prefix="")
                        request_keys = sorted(request_keys)
                        request_keys = ",".join(request_keys)

                        if request_keys not in ALL_REQUEST_KEYS_SET:
                            ALL_REQUEST_KEYS_SET.add(request_keys)
                            print("\n==> payload request")
                            pprint(payload_request)
                            testcases += 1

                    print("\nTestcases generated - ", testcases)


generate("dc30df82ef6e4007ac272b80f401a496")
