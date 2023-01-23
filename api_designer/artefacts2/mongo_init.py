from pprint import pprint
from api_designer.artefacts2.mongo_data import GetMongoData
from api_designer import mongo

TESTCASE_COLLECTION = "testcases"

class TestdataGenerator:
    def __init__(self, projectid, db, type):
        print("Mongo Artefacts Init --->")
        self.projectid = projectid
        self.db = db
        self.type = type
        self.schema_data = {}

    def fetch_data(self):
        self.project_data =  self.db.projects.find_one({"projectId": self.projectid})
        self.operation_data = self.db.operationdatas.find({"projectid": self.projectid})
        self.operation_data = list(self.operation_data)
        self.operation_data = [x["data"] for x in self.operation_data]

        self.dbdata = self.db.dbdata.find({"projectid": self.projectid})
        self.dbdata = list(self.dbdata)


        db_collection = self.db["mongo_collections"]
        schema_data = db_collection.find({"projectid": self.projectid})
        schema_data = list(schema_data)
        for sd in schema_data:
            self.schema_data[sd["collection"]] = sd["attributes"]

    def generate_db_type_functional(self):
        try:
            testdata = []
            GMD = GetMongoData(self.projectid, self.dbdata, self.db)
            testcount = 1

            for op in self.operation_data:
                endpoint = op["endpoint"]
                method = op["method"]
                request_data = op["requestData"]
                response_data = op["responseData"]

                for resp in response_data:
                    GMD.flush_data()
                    GMD.set_operation_data(method, resp["status_code"])

                    req_data = GMD.generate_request_data(request_data, self.schema_data)
                    res_data = GMD.generate_response_data(resp, req_data)

                    testdata.append({
                        "projectid": self.projectid,
                        "api_ops_id": self.projectid,
                        "filename": None,
                        "endpoint": [endpoint],
                        "method": method,
                        "resource": op.get("tags", []),
                        "operation_id": op.get("operationId"),
                        "test_case_name": op.get("operationId") + "__P",
                        "description": "ok",
                        "test_case_type": "F",
                        "delete": False,
                        "inputData": [req_data],
                        "status": res_data["status"],
                        "assertionData": [res_data["content"]],
                        "testcaseId": testcount,
                        "mock": False,
                        "isExecuted": False,
                        "endpoint_orig": endpoint
                    })
                    testcount += 1
            mongo.delete_bulk_query(TESTCASE_COLLECTION, {"projectid": self.projectid, "test_case_type": "F", "mock": False}, self.db)
            mongo.store_bulk_document(TESTCASE_COLLECTION, testdata, self.db)
            return True, "ok"

        except Exception as e:
            return False, str(e)

    def generate_db_type_performance(self):
        try:
            testdata = []
            GMD = GetMongoData(self.projectid, self.dbdata, self.db)
            testcount = 1

            for op in self.operation_data:
                endpoint = op["endpoint"]
                method = op["method"]
                request_data = op["requestData"]
                response_data = op["responseData"]

                for resp in response_data:
                    input_data = []
                    assertion_data = []
                    res_status = None

                    for _ in range(10):
                        GMD.flush_data()
                        GMD.set_operation_data(method, resp["status_code"])

                        req_data = GMD.generate_request_data(request_data, self.schema_data)
                        res_data = GMD.generate_response_data(resp, req_data)
                        input_data.append(req_data)
                        assertion_data.append(res_data["content"])
                        res_status = res_data["status"]


                    testdata.append({
                        "projectid": self.projectid,
                        "api_ops_id": self.projectid,
                        "filename": None,
                        "endpoint": [endpoint],
                        "method": method,
                        "resource": op.get("tags", []),
                        "operation_id": op.get("operationId"),
                        "test_case_name": op.get("operationId") + "__P",
                        "description": "ok",
                        "test_case_type": "P",
                        "delete": False,
                        "inputData": input_data,
                        "status": res_data["status"],
                        "assertionData": assertion_data,
                        "testcaseId": testcount,
                        "mock": False,
                        "isExecuted": False,
                        "endpoint_orig": endpoint
                    })
                    testcount += 1
            mongo.delete_bulk_query(TESTCASE_COLLECTION, {"projectid": self.projectid, "test_case_type": "F", "mock": False}, self.db)
            mongo.store_bulk_document(TESTCASE_COLLECTION, testdata, self.db)
            return True, "ok"

        except Exception as e:
            return False, str(e)


    def generate_testdata(self):
        print("generating test data")
        if self.type == "functional":
            success, message = self.generate_db_type_functional()
        else:
            success, message = self.generate_db_type_performance()
        return {"success": True, "message": "message", "status": 200 if "success" else 500}