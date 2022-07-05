from api_designer import mongo
from pprint import pprint

class Updater:
    def __init__(self, source_data):
        self.source_data = source_data

    def handle_field(self, data):
        ret = data
        if isinstance(data, str) and data.startswith("placeholder") and len(data.split("_._")) == 4:
            _, key, column_name, data_id = data.split("_._")
            if data_id in self.source_data and column_name in self.source_data[data_id]:
                return self.source_data[data_id][column_name]
        return ret

    def handle_dict(self, data):
        ret = {}
        for k, v in data.items():
            ret[k] = self.handle_object(v)
        return ret

    def handle_list(self, data):
        ret = []
        for item in data:
            ret.append(self.handle_object(item))
        return ret

    def handle_object(self, obj):
        ret = None
        if isinstance(obj, dict):
            ret = self.handle_dict(obj)
        elif isinstance(obj, list):
            ret = self.handle_list(obj)
        else:
            ret = self.handle_field(obj)
        return ret

class TestdataUpdator:
    def __init__(self, projectid, db):
        self.projectid = projectid
        self.db = db

    def fetch_data(self):
        self.testcase_data = self.db.testcases.find({"projectid": self.projectid, "mock": False})
        self.dbdata = self.db.dbdata.find({"projectid": self.projectid})
        self.source_data = {}

        for dd in self.dbdata:
            functional_data = dd["functional_data"]
            performance_data = dd["performance_data"]

            for fd in functional_data:
                if "ezapi-data-id" in fd:
                    self.source_data[fd["ezapi-data-id"]] = fd

            for pd in performance_data:
                if "ezapi-data-id" in pd:
                    self.source_data[fd["ezapi-data-id"]] = pd

    def update_testcases(self):
        for td in self.testcase_data:
            input_data = td["inputData"]
            assertion_data = td["assertionData"]
            testcaseId = td["testcaseId"]
            endpoint = td["endpoint"]

            U = Updater(self.source_data)
            new_input_data = U.handle_object(input_data)
            new_assertion_data = U.handle_object(assertion_data)
            new_final_end_point = []

            for index, item in enumerate(endpoint):
                new_end_point = str(endpoint[index]).format(**new_input_data[index]["path"])
                new_final_end_point.append(new_end_point)

            # print("\n",td["endpoint"], td["method"])
            # pprint(new_input_data)
            # pprint(new_assertion_data)
            

            # mongo update
            mongo.update_document(
                "testcases",
                {"projectid": self.projectid, "mock": False, "testcaseId": testcaseId},
                {"$set": {"inputData": new_input_data, "assertionData": new_assertion_data, "endpoint":new_end_point}},
                self.db,
            )

        return {"success": True, "message": "ok", "status": 200}