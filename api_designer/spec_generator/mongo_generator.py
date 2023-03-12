from api_designer.dbgenerate.generator import Generator

import random
import shortuuid

reference_data = {}

def dict_merge(dict1, dict2):
    return dict1.update(dict2)

class MongoGenerator:
    def __init__(self, projectid, db, schema_data = None):
        self.projectid = projectid
        self.db = db
        self.schema_data = schema_data
    
    def generate_field(self, data, name=None):
        ret = {}
        arg_type, arg_format = None, None

        if data["ezapi_type"] in ("integer", "biginteger", "float"):
            arg_type = "number"
        elif data["ezapi_type"] in ("date", "timestamp"):
            arg_type = "datetime"
        else:
            arg_type = data["ezapi_type"]
        arg_format = data["ezapi_type"]

        values = {
                "type": arg_type,
                "format": arg_format
            }
        ret[name] = values
        return ret

    def generate_object(self, data):
        ret = {}
        for k, v in data.items():
            v_type = v["ezapi_type"]

            if v_type == "object":
                ret[k] = self.generate_object(v["ezapi_object"])
            elif v_type == "array":
                ret[k] = self.generate_list(v["ezapi_array"], name=k)
            elif v_type == "oid":
                pass
            else:
                ret[k] = self.generate_field(v, name=k)[k]

        return ret

    def generate_list(self, data):
        ret = {"type": "array", "items": {}}
    
        v_type = data["ezapi_type"]
        
        if v_type == "object":
            dict_merge(ret["items"],self.generate_object(data["ezapi_object"]))

        elif v_type == "array":
            dict_merge(ret["items"],self.generate_list(data["ezapi_array"]))

        else:
            tmp = {
                "type":data["ezapi_type"]
            }
            dict_merge(ret["items"],tmp)

        return ret

    def generate_mongo_object(self, field_operation_data):
        ret = {"type": "object", "properties": {}}
        field_key = field_operation_data["key"]
        field_name = field_operation_data["sourceName"]

        field_key = field_key.split(".", 1)
        field_collection = field_key[0]
        field_ref = f"{field_key[0]}." if len(field_key) > 1 else field_name
        
        field_schema = self.schema_data[field_collection]
        field_data = field_schema

        for fr in field_ref.split("."):
            field_data = field_data[fr]
        field_type = field_data.get("ezapi_type")

        if field_type == "oid":
            ret = shortuuid.uuid()
        elif field_type == "object":
            objj = {}
            # objj[field_name] = self.generate_object(field_data["ezapi_object"])
            dict_merge(ret["properties"], self.generate_object(field_data["ezapi_object"]))
        elif field_type == "array":
            objj = {}
            objj[field_name] = self.generate_list(field_data["ezapi_array"])
            dict_merge(ret["properties"], objj)
        else:
            dict_merge(ret["properties"], self.generate_field(field_data, name = field_name))

        return ret

