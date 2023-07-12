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
    #print("source..", source)
    required_info = list(get_all_required_info(source))
    #print("required_info..", required_info)
    selected = [x[0] for x in required_info if x[1] or random.randint(0, 1)]
    #print("selected..", selected)
    #print("data..", data)
    res = get_subset_json(data, selected)
    #print("res..", res)
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
        self.dependent_data_id = ""
        self.composite_keys = []
        self.param_data = []



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
        
    def identify_data_for_get_testcases(self, item, origEzdataid):
        tmp_get_item = {}
        if "dependent-data-id" in item and item["ezapi-data-id"] == origEzdataid:
            self.dependent_data_id = item["dependent-data-id"]
            tmp_get_item = item
        elif self.dependent_data_id == item["ezapi-data-id"]:
            tmp_get_item = item
        return tmp_get_item

    def get_ezapi_data_id(self, placeholder_value):
        tmp = placeholder_value.split("._")
        if len(tmp) > 1:
            data_id = tmp[-1]
        else:
            data_id = placeholder_value
        return data_id

    def get_array_table_ref_data(self, table_ref, checking_dict):
        rets = []
        ret = {}
        final_rets = []
        final_rets_non_get = []
        final_rets_get = []
        functional_obj_item = {}
        functional_obj_item_1 = {}
        table_name = table_ref["key"]
        selected_columns = [x["name"] for x in table_ref["selectedColumns"]]

        for item in self.functional[table_name]:
            flag = True
            if not checking_dict:
                functional_obj_item = item.copy()
                break
            else:
                for k, v in checking_dict.items():
                    if v.startswith("placeholder"):
                        flag = False
                        if self.method.lower() == "get":
                            origEzdataid = self.get_ezapi_data_id(v)
                            valid_item = self.identify_data_for_get_testcases(item, origEzdataid)
                            if valid_item:
                                rets.append(valid_item)
                        else:
                            rets.append(item)
                            continue
                    else:
                        if item[k] != v:
                            flag = False
                            break
                if flag:
                    rets.append(item)

        for ret_ref in rets:
            functional_obj_item = ret_ref.copy()
            for sc in selected_columns:
                if sc not in ret_ref:
                    ret_ref[sc] = "-"

            for rk, _ in ret_ref.items():
                matched_id = ret_ref['ezapi-data-id']
                if table_name in self.placeholders and rk in self.placeholders[table_name]:
                    if self.method.lower() == "post":
                        ret_ref[rk] = "-"
                    else:
                        if rk in checking_dict:
                            ret_ref[rk] = checking_dict[rk]
                        else:
                            ret_ref[rk] = f"placeholder_._{table_name}_._{rk}_._{matched_id}"

            #ret.pop("ezapi-data-id", None)
            #ret.pop("dependent-data-id", None)

            ret_ref = {x: ret_ref[x] for x in ret_ref.keys() & selected_columns}

            final_rets_get.append(ret_ref)
        if table_name in self.request_body_table_matching:
            for i in range(len(self.request_body_table_matching[table_name])):
                ret = {}
                for rk, _ in functional_obj_item.items():
                    if table_name in self.request_body_table_matching and rk in \
                            self.request_body_table_matching[table_name][i]:
                        ret[rk] = self.request_body_table_matching[table_name][i][rk]
                    else:
                        #matched_id = functional_obj_item['ezapi-data-id']
                        if table_name in self.placeholders and rk in self.placeholders[table_name]:
                            if self.method.lower() == "post":
                                ret[rk] = "-"
                            else:
                                matched_id = self.matched_row_id[table_name]
                                ret[rk] = f"placeholder_._{table_name}_._{rk}_._{matched_id}"
                        elif rk in checking_dict:
                             ret[rk] = checking_dict[rk]
                        elif self.method.lower() == "post":
                            for x in self.placeholders[table_name]:
                                ret[x] = '-'
                        elif self.method.lower() in ["put", "patch"]:
                            for x in self.placeholders[table_name]:
                                if not x in ret:
                                    if x in self.request_body_table_matching[table_name][i]:
                                            ret[x] = self.request_body_table_matching[table_name][i][x]
                                    else:
                                        ret[x] = '-'

                ret = {x: ret[x] for x in ret.keys() & selected_columns}

                final_rets_non_get.append(ret)

        if final_rets_non_get:
            final_rets = final_rets_non_get
        else:
            final_rets = final_rets_get

        return final_rets

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

                    print(table_name, table_name in self.functional, table_name in self.selection_counter, self.selection_counter[table_name], len(self.functional[table_name]))

                    rrow = self.functional[table_name][self.selection_counter[table_name]]
                    self.selection_counter[table_name] = (self.selection_counter[table_name] + 1) % len(self.functional[table_name])

                # matched_id = rrow['ezapi-data-id']
                #
                # if field_name not in rrow:
                #     ret = shortuuid.uuid()
                #     for ft in range(len(self.functional[table_name])):
                #         if self.functional[table_name][ft]["ezapi-data-id"] == matched_id:
                #             # self.functional[table_name][ft][field_name] = ret
                #             break
                # else:
                #     ret = rrow[field_name]
                # self.matched_row_id[table_name] = matched_id
                if isinstance(rrow, list):
                    print("entered:")
                    matched_id = rrow[0]['ezapi-data-id']
                    if field_name not in rrow[0]:
                        ret = shortuuid.uuid()
                        for ft in range(len(self.functional[table_name])):
                            if self.functional[table_name][ft][0]["ezapi-data-id"] == matched_id:
                                # self.functional[table_name][ft][field_name] = ret
                                break
                    else:
                        ret = rrow[0][field_name]
                    self.matched_row_id[table_name] = matched_id

                else:
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

    def get_object_data(self, object_ref, checking_dict):
        ret = {}
        for k, v in object_ref["properties"].items():
            v_type = v.get("type")

            if v_type == "ezapi_table":
                if v.get("isArray"):
                    ret[k] = self.get_array_table_ref_data(v, checking_dict)
                ret[k] = self.get_table_ref_data(v)
            elif v_type == "object" and "properties" in v:
                ret[k] = self.get_object_data(v, checking_dict)
            elif v_type in DATA_TYPE_LIST:
                ret[k] = self.get_field_data(v)
        return ret

    def get_body_data(self, body, req_data):
        ret = None
        checking_dict = {}
        if not body:
            return ret
        if len(req_data['query']) > 0:
            for k, v in req_data['query'].items():
                checking_dict[k] = v

        if len(req_data['path']) > 0:
            for k, v in req_data['path'].items():
                checking_dict[k] = v
        body_type = body.get("type")
        if body_type == "ezapi_table":
            if body.get("isArray"):
                ret = self.get_array_table_ref_data(body, checking_dict)
                return ret
            ret = self.get_table_ref_data(body)
        elif body_type == "object" and "properties" in body:
            ret = self.get_object_data(body, checking_dict)
        elif body_type in DATA_TYPE_LIST:
            ret = self.get_field_data(body)

        return ret


    # -------------------- Request Body --------------------
    def get_request_object_data(self, object_ref, DBG):
        ret = {}
        table_columns = {}

        for k, v in object_ref["properties"].items():
            v_type = v.get("type")


            if v_type == "ezapi_table":
                table_name = v["key"]
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
                table_name = v["key"]
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

    def get_composite_keys(self, table_name):
        DBG = DBGenerator(self.projectid, None, self.db)
        DBG.fetch_table_data()
        composite_array = DBG.fetch_composite_keys(table_name)
        return composite_array


    def get_comp_key_values(self, rk, table_name):
        partial_composite_array = []
        final_dict = {}
        comp_key_vals = []
        for x in self.composite_keys:
            if x != rk:
                partial_composite_array.append(x)

        for each_col in partial_composite_array:
            for each_dict in self.param_data:
                if each_col in each_dict:
                    final_dict[each_col] = each_dict[each_col]

        dd_id = ""
        tmp = ""
        for db_data_rec in self.dbdata[table_name]['functional']:
            for k, v in db_data_rec.items():
                if k in final_dict:
                    tmp = self.get_ezapi_data_id(final_dict[k])
                if tmp == v or dd_id == v:
                    comp_key_vals.append(db_data_rec[rk])
                    if "dependent-data-id" in db_data_rec:
                        dd_id = db_data_rec["dependent-data-id"]
                        break
        return comp_key_vals

    def get_matched_ids(self, table_name):
        ezapi_data_id_array = []
        for each_dict in self.param_data:
            for k, v in each_dict.items():
                if v.startswith("placeholder"):
                    v = self.get_ezapi_data_id(v)
                    k = "ezapi-data-id"
                for db_data_rec in self.dbdata[table_name]['functional']:
                    if k in db_data_rec and db_data_rec[k] == v:
                        ezapi_data_id_array.append(db_data_rec['ezapi-data-id'])
        return ezapi_data_id_array

    def get_request_body_data(self, body):
        DBG = DBGenerator(self.projectid, None, self.db)
        DBG.fetch_table_data()
        ret = {}
        rets = []
        gen_array = []
        if not body:
            return ret

        body_type = body.get("type")

        if body_type == "ezapi_table":
            table_name = body["key"]
            self.composite_keys = self.get_composite_keys(table_name)
            print("*CompKeys*", self.composite_keys)
            isArray = body["isArray"]
            if isArray:
                tmp_data_ids = self.get_matched_ids(table_name)
                print("paramdata", self.param_data)
                print("tmp_data_ids", tmp_data_ids)
                for i in range(2):
                    gen = DBG.generate_testcase_data(table_name, [x["sourceName"] for x in body["selectedColumns"]])
                    for rk, rv in gen.items():
                        if table_name in self.placeholders and rk in self.placeholders[table_name]:
                            gen[rk] = "-"
                            if rv == "placeholder" and len(tmp_data_ids)>0:
                                gen[rk] = f"placeholder_._{table_name}_._{rk}_._{tmp_data_ids[i]}"  # todo
                            # gen[rk] = f"placeholder.{table_name}.{rk}.{matched_id}"   # todo
                        elif rk in self.composite_keys:
                            #ret[rk] = ""  # should get data from db_data
                            comp_key_vals_array = self.get_comp_key_values(rk, table_name)
                            if len(comp_key_vals_array)>0:
                                gen[rk] = comp_key_vals_array[i]

                        ret[rk] = gen[rk]
                    rets.append(ret)
                    ret = {}
                    gen_array.append(gen)

                if table_name not in self.request_body_table_matching:
                    self.request_body_table_matching[table_name] = {}
                self.request_body_table_matching[table_name] = gen_array
                self.param_data = []
                return rets
            else:
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
        if ret:
            self.param_data.append(ret)
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
                if isinstance(ret["body"], list):
                    ret_body = ret["body"][0]
                    rbody_1 = handle_required(ret_body, request_data["body"])
                    ret_body = ret["body"][1]
                    rbody_2 = handle_required(ret_body, request_data["body"])

                    tmp = {
                        "path": ret["path"],
                        "query": ret["query"],
                        "header": ret["header"],
                        "form": ret["form"],
                        "body": [rbody_1, rbody_2]
                    }

                elif isinstance(ret["body"], dict):
                    ret_body = ret["body"]

                    rbody = handle_required(ret_body, request_data["body"])

                    tmp = {
                        "path": ret["path"],
                        "query": ret["query"],
                        "header": ret["header"],
                        "form": ret["form"],
                        "body": rbody
                    }
                if tmp not in rets:
                    rets.append(tmp)
            self.param_data = []
            return rets
        self.param_data = []
        return ret

    def generate_response_data(self, resp, req_data):
        ret = None
        if resp["status_code"] == "default" or resp["status_code"].startswith(
                "2"
            ):
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