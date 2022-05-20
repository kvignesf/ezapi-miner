from pprint import pprint
import random
import shortuuid

from api_designer.dbgenerate.db_generator import DBGenerator

DATA_TYPE_LIST = ["integer", "number", "string", "boolean"]

def get_all_keys(obj):
    for k,v in obj.items():
        yield k
        if isinstance(v, dict):
            for k2 in get_all_keys(v):
                yield f"{k}.{k2}"

def get_all_required_info(obj, prefix = None):
    if not obj:
        return

    if obj.get("type") == "object" and "properties" in obj:
        for k,v in obj["properties"].items():
            v_type = v["type"]
            v_required = v.get("required", True)

            if prefix:
                yield (f"{prefix}.{k}", v_required)
            else:
                yield (k, v_required)

            if v_type == "object":
                get_all_required_info(v, prefix=k)

            elif v_type == "ezapi_table":
                for sc in v["selectedColumns"]:
                    sc_required = sc.get("required", True)

                    if prefix:
                        yield (f"{prefix}.{k}.{v['name']}", sc_required)
                    else:
                        yield (f"{k}.{sc['name']}", sc_required)

    elif obj.get("type") == "ezapi_table":
        for sc in obj["selectedColumns"]:
            sc_required = sc.get("required", True)
            if prefix:
                yield (f"{prefix}.{sc['name']}", sc_required)
            else:
                yield (sc["name"], sc_required)

    elif obj.get("type") in DATA_TYPE_LIST:
        obj_required = obj.get("required", True)
        if prefix:
            yield (f"{prefix}.{obj['name']}", obj_required)
        else:
            yield (obj["name"], obj_required)

def get_subset_json(obj, fields, prefix = None):
    ret = {}
    if not obj:
        return ret

    for k, v in obj.items():
        tmp = f"{prefix}.{k}" if prefix else k
        if tmp in fields:
            if isinstance(v, dict):
                ret[k] = get_subset_json(v, fields, prefix = k)
            else:
                ret[k] = v
    return ret

def handle_required(data, source):
    # data_keys = list(get_all_keys(data))
    required_info = list(get_all_required_info(source))

    selected = [x[0] for x in required_info if x[1] or random.randint(0, 1)]
    res = get_subset_json(data, selected)
    return res

class GetTableData:
    def __init__(self, projectid, dbdata, db, generation_type = "functional", selection_type = "random"):
        self.projectid = projectid
        self.matched_row_id = {}    # table name: id
        self.dbdata = dbdata        # dictionary key: {master, data, functional}
        self.functional = {}        # list of dictionary
        self.db = db
        self.placeholders = {}
        self.request_body_table_matching = {}
        self.ezapi_data_id_mapping = {} # To replace placeholders with inserted value
        self.selection_type = selection_type     # or incremental
        self.selection_counter = {} # table_name, counter
        self.generation_type = generation_type

        for tk, tv in self.dbdata.items():
            if not tv["master"]:
                self.functional[tk] = tv["functional"] if self.generation_type == "functional" else tv["performance"]
            else:
                tmp = tv['data']
                columns = tmp[0]
                column_data = tmp[1:]

                columns.append("ezapi-data-id")
                for cd in range(len(column_data)):
                    sid = shortuuid.uuid()
                    column_data[cd].append(sid)
                
                self.functional[tk] = [dict(zip(columns, x)) for x in column_data]

            self.placeholders[tk] = tv.get("placeholders", [])

    def flush_data(self):
        self.matched_row_id = {}
        self.request_body_table_matching = {}
        self.ezapi_data_id_mapping = {}

    def get_table_ref_data(self, table_ref):
        ret = None
        table_name = table_ref["key"]
        selected_columns = [x["name"] for x in table_ref["selectedColumns"]]
        matched_id = None

        if table_name in self.matched_row_id:
            srow = next(item for item in self.functional[table_name] if item["ezapi-data-id"] == self.matched_row_id[table_name])
            matched_id = srow["ezapi-data-id"]
            ret = {x: srow[x] for x in srow.keys() & selected_columns}
        else:
            if self.selection_type == "random":
                rrow = random.choice(self.functional[table_name])
            else:
                if table_name not in self.selection_counter:
                    self.selection_counter[table_name] = 0

                rrow = self.functional[table_name][self.selection_counter[table_name]]
                self.selection_counter[table_name] = (self.selection_counter[table_name] + 1) % len(self.functional[table_name])

            matched_id = rrow['ezapi-data-id']
            self.matched_row_id[table_name] = matched_id
            ret = {x: rrow[x] for x in rrow.keys() & selected_columns}

        for sc in selected_columns:
            if sc not in ret:
                ret[sc] = "-"

        # Replace placeholder fields with placeholder keyword
        for rk, _ in ret.items():
            if table_name in self.placeholders and rk in self.placeholders[table_name]:
                if self.method.lower() == "post":
                    ret[rk] = "-"
                else:
                    ret[rk] = f"placeholder_._{table_name}_._{rk}_._{matched_id}"

        # Match response body data with request body data
        for rk, _ in ret.items():
            if table_name in self.request_body_table_matching and rk in self.request_body_table_matching[table_name]:
                ret[rk] = self.request_body_table_matching[table_name][rk]

        return ret

    def get_field_data(self, field_ref, is_body = True):
        field_name = field_ref['name']
        ret = None
        matched_id = None
        if "key" in field_ref and "paramType" in field_ref and field_ref["paramType"] == "column":
            table_name = field_ref["key"]
            column_name = field_ref["sourceName"]

            if table_name in self.matched_row_id:
                srow = next(item for item in self.functional[table_name] if item["ezapi-data-id"] == self.matched_row_id[table_name])
                matched_id = srow["ezapi-data-id"]
                
                if field_name not in srow:
                    ret = shortuuid.uuid()
                    for ft in range(len(self.functional[table_name])):
                        if self.functional[table_name][ft]["ezapi-data-id"] == self.matched_row_id[table_name]:
                            # self.functional[table_name][ft][field_name] = ret
                            break
                else:
                    ret = srow[field_name]
            else:
                if self.selection_type == "random":
                    rrow = random.choice(self.functional[table_name])
                else:
                    if table_name not in self.selection_counter:
                        self.selection_counter[table_name] = 0

                    rrow = self.functional[table_name][self.selection_counter[table_name]]
                    self.selection_counter[table_name] = (self.selection_counter[table_name] + 1) % len(self.functional[table_name])

                matched_id = rrow['ezapi-data-id']

                if field_name not in rrow:
                    ret = shortuuid.uuid()
                    for ft in range(len(self.functional[table_name])):
                        if self.functional[table_name][ft]["ezapi-data-id"] == matched_id:
                            # self.functional[table_name][ft][field_name] = ret
                            break
                else:
                    ret = rrow[field_name]
                self.matched_row_id[table_name] = matched_id

            if table_name in self.placeholders and column_name in self.placeholders[table_name]:
                if self.method.lower() == "post" and is_body:
                    ret = "-"
                else:
                    ret = f"placeholder_._{table_name}_._{column_name}_._{matched_id}"

            if table_name in self.request_body_table_matching and column_name in self.request_body_table_matching[table_name]:
                ret = self.request_body_table_matching[table_name][column_name]

        elif "schemaName" in field_ref and "possibleValues" in field_ref and field_ref["schemaName"] == "global":
            ret = {field_ref['name']: random.choice(field_ref["possibleValues"])}

        return ret

    def get_object_data(self, object_ref):
        ret = {}
        for k, v in object_ref["properties"].items():
            v_type = v.get("type")

            if v_type == "ezapi_table":
                ret[k] = self.get_table_ref_data(v)
            elif v_type == "object" and "properties" in v:
                ret[k] = self.get_object_data(v)
            elif v_type in DATA_TYPE_LIST:
                ret[k] = self.get_field_data(v)
        return ret

    def get_body_data(self, body):
        ret = {}
        if not body:
            return ret
        
        body_type = body.get("type")
        if body_type == "ezapi_table":
            ret = self.get_table_ref_data(body)
        elif body_type == "object" and "properties" in body:
            ret = self.get_object_data(body)
        elif body_type in DATA_TYPE_LIST:
            ret = self.get_field_data(body)

        return ret


    # -------------------- Request Body --------------------
    def get_request_object_data(self, object_ref, DBG):
        ret = {}
        table_columns = {}

        for k, v in object_ref["properties"].items():
            v_type = v.get("type")
            table_name = v["key"]

            if v_type == "ezapi_table":
                ret[k] = DBG.generate_testcase_data(table_name, [x["sourceName"] for x in v["selectedColumns"]])
                # ret[k] = DBG.generate_testcase_data(table_name, selected_columns)

                for rk, rv in ret[k].items():
                    if table_name in self.placeholders and rk in self.placeholders[table_name]:
                        ret[k][rk] = "-"
                        # ret[k][rk] = f"placeholder.{table_name}.{rk}.{matched_id}"   # todo

                if table_name not in self.request_body_table_matching:
                    self.request_body_table_matching[table_name] = {}
                self.request_body_table_matching[table_name] = ret[k]

            elif v_type == "object" and "properties" in v:
                ret[k] = self.get_request_object_data(v, DBG)

            elif v_type in DATA_TYPE_LIST:
                if table_name not in table_columns:
                    table_columns[table_name] = []
                table_columns[table_name].append((v["sourceName"], k))

            elif "schemaName" in v and "possibleValues" in v and v["schemaName"] == "global":
                ret[k] = random.choice(v["possibleValues"])

        for tk, tv in table_columns.items():
            selected_columns = [x[0] for x in tv]
            table_name = tk
            gen = DBG.generate_testcase_data(table_name, selected_columns)

            for gk, gv in gen.items():
                if table_name in self.placeholders and gk in self.placeholders[table_name]:
                    gen[gk] = "-"
                    # gen[gk] = f"placeholder.{table_name}.{rk}.{matched_id}"   # todo

            for rk, rv in gen.items():
                ret[rk] = gen[rk]

            if table_name not in self.request_body_table_matching:
                self.request_body_table_matching[table_name] = {}
            self.request_body_table_matching[table_name] = gen

        

        return ret


    def get_request_body_data(self, body):
        DBG = DBGenerator(self.projectid, None, self.db)
        DBG.fetch_table_data()
        ret = {}
        if not body:
            return ret

        body_type = body.get("type")

        if body_type == "ezapi_table":
            table_name = body["key"]

            gen = DBG.generate_testcase_data(table_name, [x["sourceName"] for x in body["selectedColumns"]])
            # gen = DBG.generate_testcase_data(table_name, selected_columns)

            for rk, rv in gen.items():
                if table_name in self.placeholders and rk in self.placeholders[table_name]:
                    gen[rk] = "-"
                    # gen[rk] = f"placeholder.{table_name}.{rk}.{matched_id}"   # todo
                ret[rk] = gen[rk]


            if table_name not in self.request_body_table_matching:
                self.request_body_table_matching[table_name] = {}
            self.request_body_table_matching[table_name] = gen

        elif body_type == "object":
            ret = self.get_request_object_data(body, DBG)

        elif body_type in DATA_TYPE_LIST:
            if "tableName" in body:
                table_name = body["key"]
                gen = DBG.generate_testcase_data(table_name, [body["sourceName"]])

                for rk, rv in gen.items():
                    if table_name in self.placeholders and rk in self.placeholders[table_name]:
                        gen[rk] = "-"
                        # gen[rk] = f"placeholder.{table_name}.{rk}.{matched_id}"   # todo
                    ret[rk] = gen[rk]

                if table_name not in self.request_body_table_matching:
                    self.request_body_table_matching[table_name] = {}
                self.request_body_table_matching[table_name] = gen

            elif "schemaName" in body and "possibleValues" in body and body["schemaName"] == "global":
                ret = random.choice(body["possibleValues"])

        return ret

    def get_request_params_data(self, params):
        ret = {}
        for param in params:
            for k, v in param.items():
                ret[k] = self.get_field_data(v, is_body=False)
        return ret
    
    # -------------------- End --------------------

    def set_operation_data(self, method, status):
        self.method = method
        self.status = status

    def generate_request_data(self, request_data, is_performance = False):
        ret = {
            "path": self.get_request_params_data(request_data["path"]),
            "query": self.get_request_params_data(request_data["query"]),
            "header": self.get_request_params_data(request_data["header"]),
            "form": {},
            "body": self.get_request_body_data(request_data["body"])
        }

        if not is_performance:
            rets = []
            for _ in range(10):
                # rquery = handle_required(ret["query"], request_data["query"])
                rbody = handle_required(ret["body"], request_data["body"])

                tmp = {
                    "path": ret["path"],
                    "query": ret["query"],
                    "header": ret["header"],
                    "form": ret["form"],
                    "body": rbody
                }
                if tmp not in rets:
                    rets.append(tmp)

            return rets
        return ret

    def generate_response_data(self, resp):
        ret = None
        if resp["status_code"] == "default" or resp["status_code"].startswith(
                "2"
            ):
            content = resp.get("content")
            if content:
                ret = self.get_body_data(content)
        else:
            content = resp.get("content")
            if content:
                ret = self.get_body_data(content)

        response = {
            "status": resp["status_code"],
            "content": ret
        }
        return response