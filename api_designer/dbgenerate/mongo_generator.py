from api_designer.dbgenerate.generator import Generator

import random
import shortuuid

FUNCTIONAL_COUNT = 7
PERFORMANCE_COUNT = 100

reference_data = {}

class MongoGenerator:
    def __init__(self, projectid, db, schema_data = None):
        self.projectid = projectid
        self.db = db
        self.schema_data = schema_data
        

    # def fetch_collection_data(self):
    #     db_collection = self.db["mongo_collections"]
    #     schema_data = db_collection.find({"projectid": self.projectid})
    #     schema_data = list(schema_data)
    #     for sd in schema_data:
    #         self.schema_data[sd["collection"]] = sd["attributes"]

    def generate_field(self, data, name=None):
        if "ezapi_array_samples" in data:
            return random.sample(data["ezapi_array_samples"], 1)[0]

        arg_type, arg_format = None, None

        if data["ezapi_type"] in ("integer", "biginteger", "float"):
            arg_type = "number"
        elif data["ezapi_type"] in ("date", "timestamp"):
            arg_type = "datetime"
        else:
            arg_type = data["ezapi_type"]
        arg_format = data["ezapi_type"]

        try:
            tmp = {
                "decoder": {
                    "type": arg_type,
                    "format": arg_format
                },
                "sample": {
                    "samples": data["ezapi_samples"],
                    "null": 0,
                    "repeat": 1.0,
                },
                "name": name
            }
        except Exception as e:
            print("**", str(e), name, data.keys(), data["ezapi_type"])
            return None

        G = Generator(tmp, None, None)
        return G.generate_data()

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
                ret[k] = self.generate_field(v, name=k)

        return ret

    def generate_list(self, data, name = None):
        rets = []
        try:
            array_samples = data["ezapi_array_samples"]
            n_elements = random.sample(data["ezapi_array_size"], 1)[0]
        except Exception as e:
            print(f"* {str(e)} - Generating size of the array")
            n_elements = random.randint(2, 5)

        v_type = data["ezapi_type"]
        tmp = None

        for _ in range(n_elements):
            if v_type == "object":
                tmp = self.generate_object(data["ezapi_object"])
            elif v_type == "array":
                tmp = self.generate_list(data["ezapi_array"])

            rets.append(tmp)
      
        if not tmp:
            if "ezapi_type" in data and data["ezapi_type"] != "oid":
                rets = self.generate_field(data)

        return rets

    def generate_mongo_field(self, field_operation_data):
        ret = None
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
            ret = self.generate_object(field_data["ezapi_object"])
        elif field_type == "array":
            ret = self.generate_list(field_data["ezapi_array"])
        else:
            ret = self.generate_field(field_data)

        return ret

"""
    def generate_document(self, dg_counter):
        for k, v in self.schema["attributes"].items():
            if not "ezapi_type" in v: continue
            v_type = v["ezapi_type"]

            if k in self.relations_dict:
                reference = self.relations_dict[k]["reference"]
                reference_collection, reference_field = reference.split(".", 1)
                if reference_collection in reference_data:
                    to_refer = reference_data[reference_collection][dg_counter]
                    if reference_field in to_refer:
                        # print("Matched reference - ", to_refer[reference_field])
                        self.generated[k] = to_refer[reference_field]


            else:
                if v_type == "oid":
                    self.generated[k] = f"placeholder.{self.ezapi_data_id}"
                elif v_type == "object":
                    self.generated[k] = self.generate_object(v["ezapi_object"])
                elif v_type == "array":
                    self.generated[k] = self.generate_list(v["ezapi_array"], name=k)
                else:
                    self.generated[k] = self.generate_field(v, name=k)

def genearte_mongo_testcase_data(projectid, generation_type = "functional"):
    client, db = mongo.get_db_connection()
    db_collection = db["mongo_collections"]

    database_details = db["database"].find_one({"projectid": projectid})
    collection_order = database_details["order"]
    schema_data = db_collection.find({"projectid": projectid})
    schema_data = list(schema_data)

    schema_data2 = []
    for co in collection_order:
        for sd in schema_data:
            if co == sd["collection"]:
                schema_data2.append(sd)

    _dbdata = []

    for sd in schema_data2:
        datagen_count = FUNCTIONAL_COUNT if generation_type == "functional" else PERFORMANCE_COUNT

        collection_name = sd["collection"]

        document = db.dbdata.find_one({"projectid": projectid, "key": collection_name})
        new_document = True
        if document:
            new_document = False

        if new_document:
            document = {
                "projectid": projectid,
                "database": "mongo",
                "key": collection_name,
                "query": None,
                "is_master": None,
                "is_generated": False,
                "functional_data": [],
                "performance_data": [],
                "successful_inserted_ids_functional": [],
                "successful_inserted_ids_performance": [],
                "failed_inserted_ids_functional": [],
                "failed_inserted_ids_performance": [],
            }

        if generation_type == "functional":
            document["functional_data"] = []
        elif generation_type == "performance":
            document["performance_data"] = []

        for dg_counter in range(datagen_count):
            ezapi_data_id = shortuuid.uuid()
            MG = MongoGenerator(sd, ezapi_data_id)
            MG.generate_document(dg_counter)

            if collection_name not in reference_data:
                reference_data[collection_name] = []
            reference_data[collection_name].append(MG.generated)

            generated_data = {
                "data": MG.generated,
                "ezapi-data-id": ezapi_data_id
            }
            
            if generation_type == "functional":
                document["functional_data"].append(generated_data)
                document["is_generated"] = True
            elif generation_type == "performance":
                document["performance_data"].append(generated_data)
                document["is_generated"] = True

        _dbdata.append(document)
"""