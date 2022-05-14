from pprint import pprint

from email import message
from api_designer.artefacts2.table_data import GetTableData
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
        # try:
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
                req_data = GTD.generate_request_data(request_data)
                res_data = GTD.generate_response_data(resp)

                # testdata.append({
                #     "projectid": self.projectid,
                #     "api_ops_id": self.projectid,
                #     "filename": None,
                #     "endpoint": endpoint,
                #     "method": method,
                #     "resource": op.get("tags", []),
                #     "operation_id": op.get("operationId"),
                #     "test_case_name": op.get("operationId") + "__P",
                #     "description": "ok",
                #     "test_case_type": "F",
                #     "delete": False,
                #     "inputData": req_data,
                #     "status": res_data["status"],
                #     "assertionData": res_data["content"],
                #     "testcaseId": testcount,
                #     "mock": False
                # })
                # testcount += 1

                testdata.append({
                    "endpoint": endpoint,
                    "method": method,
                    "inputData": req_data,
                    "assertionData": res_data["content"],
                    "status": res_data["status"]
                })

        # mongo.store_bulk_document(TESTCASE_COLLECTION, testdata, self.db)
        return True, "ok", testdata
        # except Exception as e:
        #     return False, str(e)

    def generate_db_type_performance(self):
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
                    for _ in range(10):
                        GTD.flush_data()
                        GTD.set_operation_data(method, resp["status_code"])
                        req_data = GTD.generate_request_data(request_data)
                        res_data = GTD.generate_response_data(resp)

                        testdata.append({
                            "endpoint": endpoint,
                            "method": method,
                            "inputData": req_data,
                            "assertionData": res_data["content"],
                            "status": res_data["status"]
                        })
                    # else:
                    #     GTD.flush_data()
                    #     GTD.set_operation_data(method, resp["status_code"])
                    #     req_data = GTD.generate_request_data(request_data)
                    #     res_data = GTD.generate_response_data(resp)

                    #     for _ in range(10):
                    #         testdata.append({
                    #             "endpoint": endpoint,
                    #             "method": method,
                    #             "inputData": req_data,
                    #             "assertionData": res_data["content"],
                    #             "status": res_data["status"]
                    #         })
                
        return True, "ok", testdata


    def generate_spec_db_type(self):
        pass

    def generate_testdata(self):
        ret = None
        if self.project_data["projectType"] == "db":
            if self.type == "performance":   
                success, message, data = self.generate_db_type_performance()
            else:
                success, message, data = self.generate_db_type_functional()

        elif self.project_data["projectType"] == "both":
            success, message = self.generate_spec_db_type()

        else:
            pass
        return {"success": success, "message": message, "status": 200 if success else 500, "data": data}