from pprint import pp, pprint
from bson import json_util
from collections import Counter, defaultdict

import bson
import datetime
import decimal
import json
import pymongo

from api_designer.nosql_connect.ts2 import get_ts_order

MAXIMUM_MATCH_FIELD_LENGTH = 256
INTER_MATCH_THRESHOLD = 0.9


# Match list2 in list1
def get_list_intersections(list1, list2):
    type_l1 = [type(x) for x in list1]
    type_l2 = [type(x) for x in list2]

    max_type_l1 = max(set(type_l1), key=type_l1.count)
    max_type_l2 = max(set(type_l2), key=type_l2.count)

    list1 = [x for x in list1 if type(x) == max_type_l1]
    list2 = [x for x in list2 if type(x) == max_type_l2]

    list1 = sorted(list1)
    list2 = sorted(list2)

    n1 = len(list1)
    n2 = len(list2)

    i = 0
    j = 0

    common = 0
    unmatched = 0
    occurence = []

    current = None
    current_index = None
    current_occurence = 0

    while i < n1 and j < n2:
        try:
            if current != None and list2[j] == current and current_index != j:
                pass
            elif current != None and current_index:
                occurence.append((current, current_occurence))
                current_occurence = 0
                current = None
                current_index = None

            if list1[i] == list2[j]:
                common += 1
                current = list2[j]
                current_index = j
                current_occurence += 1
                j += 1
                # i += 1

            elif list1[i] < list2[j]:
                i += 1

            else:
                j += 1
                unmatched += 1
        except Exception as e:
            print("**", str(e))

    if current != None and current_index != None:
        occurence.append((current, current_occurence))
        current_occurence = 0
        current = None
        current_index = None

    if j < n2:
        unmatched += (n2 - j)
        j += 1

    ret = {
        "common": common,
        "unmatched": unmatched,
        "occurence": occurence
    }
    return ret


def average(lst):
    return round(sum(lst) / len(lst), 2)


def json_safe(obj):
    if type(obj) in (datetime.datetime, datetime.date, datetime.time):
        return obj.isoformat()
    elif type(obj) == decimal.Decimal:
        return float(obj)
    elif type(obj) == bson.decimal128.Decimal128:
        return float(obj.to_decimal())
    elif type(obj) == dict:
        for (k, v) in obj.items():
            obj[k] = json_safe(v)
        return obj
    elif type(obj) == list:
        return [json_safe(li) for li in obj]
    elif type(obj) == memoryview:
        return obj.tobytes()
    return obj


PYMONGO_TYPE_TO_TYPE_STRING = {
    list: "ARRAY",
    dict: "OBJECT",
    type(None): "null",

    bool: "boolean",
    int: "integer",
    bson.int64.Int64: "biginteger",
    float: "float",

    str: "string",

    bson.decimal128.Decimal128: "float",

    bson.datetime.datetime: "date",
    bson.timestamp.Timestamp: "timestamp",

    bson.dbref.DBRef: "dbref",
    bson.objectid.ObjectId: "oid",
}


def get_type(value):
    value_type = type(value)
    if value_type in PYMONGO_TYPE_TO_TYPE_STRING.keys():
        value_type = PYMONGO_TYPE_TO_TYPE_STRING[value_type]
    return value_type


def default_to_regular(d):
    if isinstance(d, dict):
        d = {k: default_to_regular(v) for k, v in d.items()}
    return d


def beautify_schema(schema):
    schema = default_to_regular(schema)
    schema = json_safe(schema)
    schema = json.loads(json_util.dumps(schema))
    return schema


def check_same_dtype(type_dict):
    type_list = list(type_dict.keys())
    return type_list[0] if len(type_list) == 1 else None


def get_regex_matches(data):
    pass


class MongoExtractor:
    def __init__(self, dbtype, uri, dbname):
        self.dbtype = dbtype
        self.dbname = dbname
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[dbname]

        self.schema = {}
        self.relations = []
        self.internal_relations = []
        self.collection_schemas = []
        self.uniques = {}
        self.potential_match_fields = {}

    def get_collection_data(self):
        self.collections = self.db.list_collection_names()
        for c in self.collections:
            self.schema[c] = self.extract_collection(c)

        self.schema = beautify_schema(self.schema)

    def extract_collection(self, c, sample_size=300):
        db_collection = self.db[c]
        n_documents = db_collection.count_documents({})

        documents = db_collection.aggregate([{'$sample': {'size': sample_size}}], allowDiskUse=True)
        collection_schema = self.init_object()
        for document in documents:
            self.add_document(document, collection_schema)

        return collection_schema

    @staticmethod
    def init_object():
        def init_field():
            empty_field = {
                "ezapi_type2": defaultdict(int),
                "ezapi_type": None,
                "ezapi_count": 0
            }
            return empty_field

        empty_object = defaultdict(init_field)
        return empty_object

    def add_document(self, document, schema):
        for k, v in document.items():
            self.add_value(v, schema[k])

        return schema

    def add_value(self, value, schema):
        schema['ezapi_count'] += 1

        if isinstance(value, dict):
            self.add_object(value, schema)
        elif isinstance(value, list):
            self.add_list(value, schema)
        else:
            self.add_field(value, schema)

        return schema

    def add_object(self, value, schema, type_str="ezapi_type2"):
        schema[type_str]["object"] += 1
        schema["ezapi_type"] = max(schema[type_str], key=schema[type_str].get)
        if "ezapi_object" not in schema:
            schema["ezapi_object"] = self.init_object()
        self.add_document(value, schema["ezapi_object"])

    def add_list(self, value, schema, type_str="ezapi_type2"):
        schema[type_str]["array"] += 1
        schema["ezapi_type"] = max(schema[type_str], key=schema[type_str].get)
        if "ezapi_array" not in schema:
            schema["ezapi_array"] = self.init_object()

        if "ezapi_array_size" not in schema["ezapi_array"]:
            schema["ezapi_array"]["ezapi_array_size"] = []
            schema["ezapi_array"]["ezapi_array_samples"] = []

        schema["ezapi_array"]["ezapi_array_size"].append(len(value))

        is_all_direct_field = True
        for v in value:
            if not (isinstance(v, dict) or isinstance(v, list)):
                value_type = get_type(v)
                if not type_str in schema["ezapi_array"]:
                    schema["ezapi_array"][type_str] = defaultdict(int)
                schema["ezapi_array"][type_str][value_type] += 1
                schema["ezapi_array"]["ezapi_type"] = max(schema["ezapi_array"][type_str],
                                                          key=schema["ezapi_array"][type_str].get)

            elif isinstance(v, dict):
                if not type_str in schema["ezapi_array"]:
                    schema["ezapi_array"][type_str] = defaultdict(int)
                schema["ezapi_array"][type_str]["object"] += 1
                schema["ezapi_array"]["ezapi_type"] = max(schema["ezapi_array"][type_str],
                                                          key=schema["ezapi_array"][type_str].get)
                schema["ezapi_array"]["ezapi_object"] = self.init_object()

                self.add_document(v, schema["ezapi_array"]["ezapi_object"])

        if is_all_direct_field:
            schema["ezapi_array"]["ezapi_array_samples"].append(value)

    def add_field(self, value, schema, type_str="ezapi_type2"):
        value_type = get_type(value)
        schema[type_str][value_type] += 1
        schema["ezapi_type"] = max(schema[type_str], key=schema[type_str].get)

        if "ezapi_samples" not in schema:
            schema["ezapi_samples"] = []

        if value_type == "oid":
            value = str(value)
        schema["ezapi_samples"].append(value)

    def get_unique_fields(self):
        for ck, cv in self.schema.items():
            unique_ids = []
            match_fields = []
            for k, v in cv.items():
                if 'ezapi_count' in v and 'ezapi_type2' in v and 'ezapi_samples' in v:
                    v_type = check_same_dtype(v["ezapi_type2"])
                    if v_type in ("string", "integer", "biginteger"):
                        samples = v["ezapi_samples"]

                        if v_type == "string":
                            maxm_length = max([len(x) for x in samples])
                            if maxm_length <= MAXIMUM_MATCH_FIELD_LENGTH:
                                match_fields.append(k)
                        else:
                            match_fields.append(k)

                        if (len(samples) * 0.95 <= len(set(samples))) and None not in samples and "" not in samples:
                            tmp = [False if " " in str(x) else True for x in samples]
                            tmp = all(tmp)

                            if tmp:
                                unique_ids.append((k, v_type))

                    elif v_type == "oid":
                        match_fields.append(k)
                        samples = v["ezapi_samples"]
                        # samples = [x["$oid"] for x in samples]
                        if (len(samples) == len(set(samples))):
                            unique_ids.append((k, v_type))

            self.uniques[ck] = unique_ids
            self.potential_match_fields[ck] = match_fields

    def get_relations(self):
        new_collection_data = {}
        for pk, pv in self.potential_match_fields.items():
            selected_fields = {x: 1 for x in pv}
            db_collection = self.db[pk]
            db_documents = db_collection.find({}, selected_fields)
            db_documents = list(db_documents)
            db_documents = json_safe(db_documents)
            db_documents = json.loads(json_util.dumps(db_documents))

            new_document = {}
            for dd in db_documents:
                for dk, dv in dd.items():
                    if dk not in new_document:
                        new_document[dk] = []
                    if type(dv) == bson.objectid.ObjectId:
                        new_document[dk].append(dv["$oid"])
                    else:
                        new_document[dk].append(dv)

            new_collection_data[pk] = new_document

        for uk, uv in self.uniques.items():
            for field in uv:
                field_name = field[0]
                field_type = field[1]
                field_samples = None

                if field_name in new_collection_data[uk]:
                    field_samples = new_collection_data[uk][field_name]

                if not field_samples: continue
                if field_type == "oid":
                    field_samples = [x["$oid"] for x in field_samples]

                for ck, cv in new_collection_data.items():
                    if ck != uk:
                        for to_match_field, to_match_samples in cv.items():
                            if not to_match_samples: continue

                            if type(to_match_samples[0]) == dict:
                                to_match_samples = [x["$oid"] for x in to_match_samples]
                                to_match_type = "oid"
                            else:
                                to_match_type = type(to_match_samples[0])
                                to_match_type = PYMONGO_TYPE_TO_TYPE_STRING[to_match_type]

                            if field_type != to_match_type: continue

                            match_res = get_list_intersections(field_samples, to_match_samples)

                            common_match = match_res["common"]
                            unmatched = match_res["unmatched"]
                            occurence = match_res["occurence"]
                            matched_ratio = int(round((common_match * 100) / (common_match + unmatched), 0))

                            if matched_ratio >= 80:
                                occurence_values = [x[1] for x in occurence]
                                max_occurence = max(occurence_values)
                                min_occurence = min(occurence_values)
                                avg_occurence = average(occurence_values)

                                print(f"{uk}.{field_name} == {ck}.{to_match_field} - {matched_ratio}")
                                print(max_occurence, min_occurence, avg_occurence)

                                relation_type = "OneToMany" if avg_occurence > 1 else "OneToOne"
                                relation_key = f"{uk}.{field_name}"
                                relation_value = f"{ck}.{to_match_field}"
                                relation_object = {
                                    "key": relation_value,
                                    "reference": relation_key,
                                    "type": relation_type,
                                    "match": matched_ratio
                                }

                                if relation_type == "OneToMany":
                                    relation_object["occurence"] = occurence
                                self.relations.append(relation_object)

    def get_internal_relations(self, sample_size=100):
        for c in self.collections:
            db_collection = self.db[c]
            documents = db_collection.aggregate([{'$sample': {'size': sample_size}}], allowDiskUse=True)
            documents = [x for x in documents]
            n_documents = len(documents)

            # occurence of unique fields
            for uk, uv in self.uniques.items():
                if uk != c: continue

                for field in uv:
                    field_name = field[0]
                    field_type = field[1]

                    matches = {}
                    for doc in documents:
                        if field_name not in doc: continue
                        to_match = str(doc[field_name])
                        for dk, dv in doc.items():
                            if dk == field_name: continue
                            if isinstance(dv, str) and to_match in dv:
                                if dk not in matches:
                                    matches[dk] = {
                                        "match": 0,
                                        "value": []
                                    }
                                matches[dk]["match"] += 1
                                matches[dk]["value"].append((to_match, dv))

                    # filter matches
                    for mk, mv in matches.items():
                        if mv and mv["match"] and mv["match"] >= INTER_MATCH_THRESHOLD * n_documents:
                            print("Relation found - ", mk)
                        else:
                            print("Pass - ", mk)

            # array length match
            matches2 = {}
            for doc in documents:
                for dk1, dv1 in doc.items():
                    for dk2, dv2 in doc.items():
                        if dk1 == dk2: continue
                        if isinstance(dv1, int) and isinstance(dv2, list) and dv1 == len(dv2):
                            if dk1 not in matches2:
                                matches2[dk1] = {
                                    "match": 0,
                                    "reference": None
                                }
                            matches2[dk1]["match"] += 1
                            matches2[dk1]["reference"] = dk2

                            # filter matches
            for mk, mv in matches2.items():
                if mv and mv["match"] and mv["match"] >= INTER_MATCH_THRESHOLD * n_documents:
                    print(mk, mv["match"], mv["reference"])

    def get_insertion_order(self):
        collection_dependencies = []
        for c in self.collections:
            foreign_dependencies = set()
            for r in self.relations:
                if r["type"] == "OneToMany" and r["key"].split(".")[0] == c:
                    foreign_dependencies.add(r["reference"].split(".")[0])

            collection_dependencies.append({
                "key": c,
                "dependencies": list(foreign_dependencies)
            })
        print(collection_dependencies)
        self.insertion_order = get_ts_order(collection_dependencies)

    def prepare_db_document(self):
        document = {
            'projectid': self.projectid,
            'type': self.dbtype,
            'collections': self.collections,
            'order': self.insertion_order
        }
        return document

    def prepare_schema_documents(self):
        for k, v in self.schema.items():
            document = {
                "projectid": self.projectid,
                "dbname": self.dbname,
                "collection": k,
                "relations": [],
                "attributes": v
            }

            for rel in self.relations:
                rel_collection = rel["key"].split(".")[0]
                if k == rel_collection:
                    document["relations"].append(rel)

            self.collection_schemas.append(document)
        return self.collection_schemas

    def extract_data(self, projectid):
        self.projectid = projectid
        self.get_collection_data()
        self.get_unique_fields()
        self.get_relations()
        self.get_internal_relations()

        # print(self.collections)
        # print(self.relations)

        self.get_insertion_order()

        # pprint(self.uniques)
        # pprint(self.potential_match_fields)
        db_document = self.prepare_db_document()
        collection_document = self.prepare_schema_documents()
        return db_document, collection_document