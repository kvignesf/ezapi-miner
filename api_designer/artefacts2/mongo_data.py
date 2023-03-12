from pprint import pprint
import random

from api_designer.dbgenerate.mongo_generator import MongoGenerator

class GetMongoData:
    def __init__(self, projectid, dbdata, db, generation_type = "functional", selection_type = "random"):
        self.projectid = projectid
        self.dbdata = dbdata
        self.db = db
        self.generation_type = generation_type
        self.selection_type = selection_type # or incremental

        self.functional = {} # collection_name, collection_data
        self.matched_row_id = {}
        self.selection_counter = {}

        # No concept of master collection or placeholder
        for dd in self.dbdata:
            dk = dd["key"]
            self.functional[dk] = dd["functional_data"] if self.generation_type == "functional" else dd["performance_data"]

    def flush_data(self):
        self.matched_row_id = {}

    def set_operation_data(self, method, status):
        self.method = method
        self.status = status

    def get_field_data(self, field_ref):
        ret = None
        field_name = field_ref["name"]
        # matched_id = None

        if "key" in field_ref and "paramType" in field_ref and field_ref["paramType"] == "documentField":
            collection_name = field_ref["key"]
            document_field_name = field_ref["sourceName"]

            if collection_name in self.matched_row_id:
                srow = next(item for item in self.functional[collection_name] if item["ezapi-data-id"] == self.matched_row_id[collection_name])
                # matched_id = srow["ezapi-data-id"]
                ret = srow[field_name]
            else:
                if self.selection_type == "random":
                    rrow = random.choice(self.functional[collection_name])
                else:
                    if collection_name not in self.selection_counter:
                        self.selection_counter[collection_name] = 0

                    rrow = self.functional[collection_name][self.selection_counter[collection_name]]
                    self.selection_counter[collection_name] = (self.selection_counter[collection_name] + 1) % len(self.functional[collection_name])

                matched_id = rrow['ezapi-data-id']
                self.matched_row_id[collection_name] = matched_id
                rrow = rrow["data"]
                ret = rrow[field_name]

        return ret

    def get_object_data(self, object_ref, ref_dict):
        ret = {}
        for k, v in object_ref["properties"].items():
            if "paramType" in v and v["paramType"] == "documentField":
                tmp = v["key"].split(".", 1)   
                ref_collection = tmp[0] # collection name
                ref_key = (f"{tmp[1]}." if len(tmp) > 1 else "") + v["sourceName"]

                ref_value = next(item for item in self.functional[ref_collection] if item["ezapi-data-id"] == self.matched_row_id[ref_collection])
                ref_value = ref_value["data"]
                ref_key = ref_key.split(".")
                for rk in ref_key:
                    ref_value = ref_value[rk]
                ret[k] = ref_value
            elif "schemaName" in v and "possibleValues" in v and v["schemaName"] == "global":
                ret[k] = random.choice(v["possibleValues"])
        return ret


    def get_body_data(self, body, req_data):
        ret = None
        req_params_dict = {}

        if not body:
            return ret

        if len(req_data['query']) > 0:
            for k, v in req_data['query'].items():
                req_params_dict[k] = v

        if len(req_data['path']) > 0:
            for k, v in req_data['path'].items():
                req_params_dict[k] = v

        body_type = body.get('type')
        if body_type == "object" and "properties" in body:
            ret = self.get_object_data(body, req_params_dict)
        else:
            print("***** ERROR - Not Found Body Data")

        return ret

    def get_request_params_data(self, params):
        ret = {}
        for param in params:
            for k, v in param.items():
                ret[k] = self.get_field_data(v)
        return ret

    def get_request_body_data(self, body, schema_data):
        MG = MongoGenerator(self.projectid, self.db, schema_data)
        # MG.fetch_collection_data()
        ret = {} # or []
        body_type = body.get("type")

        if not body: return ret
        if body_type == "object":
            for k, v in body["properties"].items():
                print("generating ", k, " ->")
                v_type = v.get("type")
                collection_name = v["key"]
                ret[k] = MG.generate_mongo_field(v)

        return ret   

    def generate_request_data(self, request_data, schema_data):
        ret = {
            "path": self.get_request_params_data(request_data["path"]),
            "query": self.get_request_params_data(request_data["query"]),
            "header": self.get_request_params_data(request_data["header"]),
            "form": {},
            "body": self.get_request_body_data(request_data["body"], schema_data)
        }
        return ret
        # todo - required handling

    def generate_response_data(self, resp, req_data):
        ret = None
        if resp["status_code"] == "default" or resp["status_code"].startswith("2"):
            content = resp.get("content")
            if content:
                ret = self.get_body_data(content, req_data)
        else:
            content = resp.get("content")
            if content:
                ret = self.get_body_data(content, req_data)

        response = {
            "status": resp["status_code"],
            "content": ret
        }
        return response