import re
from pprint import pprint

from api_designer.artefacts2.table_data import GetTableData
from api_designer.artefacts2.schema_data import GetSchemaData
from api_designer import mongo

TESTCASE_COLLECTION = "testcases"

class TestdataGenerator:
    def __init__(self, projectid, db, type):
        self.projectid = projectid
        self.db = db
        self.type = type
        
    def fetch_data(self):
        self.project_data =  self.db.projects.find_one({"projectId": self.projectid})
        self.operation_data = self.db.operationdatas.find({"projectid": self.projectid})
        self.operation_data = list(self.operation_data)
        self.operation_data = [x["data"] for x in self.operation_data]

        self.table_data = self.db.tables.find({"projectid": self.projectid})
        self.dbdata = self.db.dbdata.find({"projectid": self.projectid})

        self.table_data = list(self.table_data)
        self.dbdata = list(self.dbdata)

    def combine_table_dbdata(self):
        ret = {}
        for td in self.table_data:
            if td['key'] not in ret:
                ret[td['key']] = {}

            generated_data = next(item for item in self.dbdata if item["key"] == td["key"])
            ret[td['key']]["master"] = td["master"]
            ret[td['key']]["data"] = td["data"]
            ret[td['key']]["functional"] = generated_data.get("functional_data") if generated_data else None
            ret[td['key']]["performance"] = generated_data.get("performance_data") if generated_data else None
            ret[td['key']]['placeholders'] = generated_data.get("placeholders", []) if generated_data else []

        return ret

    def generate_db_type_functional(self):
        try:
            testdata = []
            data = self.combine_table_dbdata()
            GTD = GetTableData(self.projectid, data, self.db, generation_type="functional", selection_type="incremental")
            testcount = 1

            for op in self.operation_data:
                endpoint = op["endpoint"]
                method = op["method"]
                request_data = op["requestData"]
                response_data = op["responseData"]

                for resp in response_data:
                    GTD.flush_data()

                    GTD.set_operation_data(method, resp["status_code"])
                    req_datas = GTD.generate_request_data(request_data)
                    res_data = GTD.generate_response_data(resp, req_datas[0])

                    for req_data in req_datas:
                        # if ("{" in endpoint and "}" in endpoint):
                        #     pathparams = re.findall(r'\{.*?\}', endpoint)
                        #     endpoint_orig = endpoint
                        #     print(endpoint.format(**req_data["path"]))
                        #     #testdata["endpoint"] = endpoint.format(**req_data["path"])
                        #     newendpoint = endpoint.format(**req_data["path"])
                        #     #testdata["endpoint_orig"] = endpoint_orig
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
            data = self.combine_table_dbdata()
            GTD = GetTableData(self.projectid, data, self.db, generation_type="performance", selection_type="incremental")
            testcount = 1

            for op in self.operation_data:
                endpoint = op["endpoint"]
                method = op["method"]
                request_data = op["requestData"]
                response_data = op["responseData"]

                for resp in response_data:
                    status_code = resp["status_code"]
                    
                    if status_code == "default" or status_code.startswith("2"):
                        input_data = []
                        assertion_data = []

                        for _ in range(10):
                            GTD.flush_data()
                            GTD.set_operation_data(method, resp["status_code"])
                            req_data = GTD.generate_request_data(request_data, is_performance = True)
                            res_data = GTD.generate_response_data(resp, req_data)

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
                            "status": res_status,
                            "assertionData": assertion_data,
                            "testcaseId": testcount,
                            "mock": False,
                            "isExecuted": False
                        })
                        testcount += 1
            mongo.delete_bulk_query(TESTCASE_COLLECTION, {"projectid": self.projectid, "test_case_type": "P", "mock": False}, self.db)
            mongo.store_bulk_document(TESTCASE_COLLECTION, testdata, self.db) 
            return True, "ok"
        except Exception as e:
            return False, str(e)


    def generate_spec_db_type_functional(self):
        try:
            testdata = []
            testcount = 1

            data = self.combine_table_dbdata()
            GSD = GetSchemaData(self.projectid, self.db, data, generation_type="functional", selection_type="incremental")

            for op in self.operation_data:
                endpoint = op["endpoint"]
                method = op["method"]
                request_data = op["requestData"]
                response_data = op["responseData"]

                for resp in response_data:
                    GSD.flush_data()
                    GSD.set_operation_data(method, resp["status_code"])
                    req_datas = GSD.generate_request_data(request_data)
                    res_data = GSD.generate_response_data(resp, req_datas[0])

                    for req_data in req_datas:
                        testdata.append({
                            "projectid": self.projectid,
                            "api_ops_id": self.projectid,
                            "filename": None,
                            "endpoint": endpoint,
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
                            "isExecuted": False
                        })
                        testcount += 1
            mongo.delete_bulk_query(TESTCASE_COLLECTION, {"projectid": self.projectid, "test_case_type": "F", "mock": False}, self.db)
            mongo.store_bulk_document(TESTCASE_COLLECTION, testdata, self.db)
            return True, "ok"
        except Exception as e:
            return False, str(e)

    def generate_spec_db_type_performance(self):
        try:
            testdata = []
            data = self.combine_table_dbdata()
            GSD = GetSchemaData(self.projectid, self.db, data, generation_type="performance", selection_type="incremental")
            testcount = 1

            for op in self.operation_data:
                endpoint = op["endpoint"]
                method = op["method"]
                request_data = op["requestData"]
                response_data = op["responseData"]

                for resp in response_data:
                    status_code = resp["status_code"]
                    
                    if status_code == "default" or status_code.startswith("2"):
                        input_data = []
                        assertion_data = []

                        for _ in range(10):
                            GSD.flush_data()
                            GSD.set_operation_data(method, resp["status_code"])
                            req_data = GSD.generate_request_data(request_data, is_performance = True)
                            res_data = GSD.generate_response_data(resp)

                            input_data.append(req_data)
                            assertion_data.append(res_data["content"])
                            res_status = res_data["status"]

                        testdata.append({
                            "projectid": self.projectid,
                            "api_ops_id": self.projectid,
                            "filename": None,
                            "endpoint": endpoint,
                            "method": method,
                            "resource": op.get("tags", []),
                            "operation_id": op.get("operationId"),
                            "test_case_name": op.get("operationId") + "__P",
                            "description": "ok",
                            "test_case_type": "P",
                            "delete": False,
                            "inputData": input_data,
                            "status": res_status,
                            "assertionData": assertion_data,
                            "testcaseId": testcount,
                            "mock": False,
                            "isExecuted": False
                        })
                        testcount += 1
            mongo.delete_bulk_query(TESTCASE_COLLECTION, {"projectid": self.projectid, "test_case_type": "P", "mock": False}, self.db)
            mongo.store_bulk_document(TESTCASE_COLLECTION, testdata, self.db) 
            return True, "ok"
        except Exception as e:
            return False, str(e)


    def generate_testdata(self):
        print("generating test data")
        if self.project_data["projectType"] == "db":
            if self.type == "performance":   
                success, message = self.generate_db_type_performance()
            else:
                success, message = self.generate_db_type_functional()
            return {"success": success, "message": message, "status": 200 if success else 500}

        elif self.project_data["projectType"] == "both":
            if self.type == "performance":
                success, message = self.generate_spec_db_type_performance()
            else:
                success, message = self.generate_spec_db_type_functional()
            return {"success": success, "message": message, "status": 200 if success else 500}