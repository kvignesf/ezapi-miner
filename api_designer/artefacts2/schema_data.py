from pprint import pprint
import random
from numpy import isin
import shortuuid
from api_designer.dbgenerate.db_generator import DBGenerator

DATA_TYPE_LIST = ["integer", "number", "string", "boolean"]

def get_all_keys(obj):
    for k,v in obj.items():
        yield k
        if isinstance(v, dict):
            for k2 in get_all_keys(v):
                yield f"{k}.{k2}"

def get_all_required_info(obj, attr_required_dict, prefix = None):
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

    elif obj.get("type") == 'ezapi_ref':
        print("schemaName:", obj.get("name"))
        print("attr_required_dict:", attr_required_dict)
        for sc in attr_required_dict[obj.get("name")]:
            if prefix:
                yield (f"{prefix}.{sc}", True)
            else:
                yield (sc, True)

    elif obj.get("type") in DATA_TYPE_LIST:
        obj_required = obj.get("required", True)
        if prefix:
            yield (f"{prefix}.{obj['name']}", obj_required)
        else:
            yield (obj["name"], obj_required)

def get_subset_json(obj, fields, prefix = None):
    print(obj, fields, prefix)
    ret = {}
    if not obj:
        return ret

    if isinstance(obj, object):
        for k, v in obj.items():
            tmp = f"{prefix}.{k}" if prefix else k
            if tmp in fields:
                if isinstance(v, dict):
                    ret[k] = get_subset_json(v, fields, prefix = k)
                else:
                    ret[k] = v
    return ret

def handle_required(data, source, attr_required_dict):
    # data_keys = list(get_all_keys(data))
    required_info = list(get_all_required_info(source, attr_required_dict))

    selected = [x[0] for x in required_info if x[1] or random.randint(0, 1)]
    res = get_subset_json(data, selected)
    return res


class GetSchemaData:
    def __init__(self, projectid, db, dbdata, generation_type = "functional", selection_type = "random"):
        self.projectid = projectid
        self.db = db
        self.dbdata = dbdata
        self.generation_type = generation_type
        self.selection_type = selection_type
        self.selection_counter = {}

        self.functional = {}
        self.matched_row_id = {} 
        self.placeholders = {}
        self.request_body_table_matching = {}
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
        
        self.user_matched = {}
        user_matches = self.db.user_ovrrd_matches.find({"projectId": self.projectid})
        for um in user_matches:
            attribute_path = um["attributePath"].replace("/", ".")
            self.user_matched[attribute_path] = {
                "key": um["key"],
                "schema": um["schemaName"],
                "schema_attribute": um["schemaAttribute"],
                "schema_attribute_level": um["attributeLevel"],
                "table": um["tableName"],
                "table_attribute": um["tableAttribute"]
            }

        # self.schemas = {}
        # schemas = self.db.schemas.find({"projectid": self.projectid})
        # for sc in schemas:
        #     schema_name = sc["data"]["name"]
        #     schema_attributes = sc["data"]["attributes"]
        #     self.schemas[schema_name] = schema_attributes

        self.schemas = {}
        self.attr_required = []
        self.schema_attr_req = {}
        schemas = self.db.components.find_one({"projectid": self.projectid})
        schemas = schemas["data"]["schemas"]
        for sk, sv in schemas.items():
            schema_name = sk
            if "type" in sv and sv["type"] == "object" and "properties" in sv:
                schema_attributes = sv["properties"]
                self.schemas[schema_name] = schema_attributes

            if "type" in sv and sv["type"] == "object" and "required" in sv:
                self.attr_required = sv["required"]
                self.schema_attr_req[schema_name] = self.attr_required


        self.ai_matched = {}
        self.ai_matches = list(self.db.matcher.find({"projectid": self.projectid, "key": {"$exists": True}}))

        self.matcher = {}
        for am in self.ai_matches:
            schema = am["schema"]
            table = am["table"]
            match_type = am["match_type"]
            # key = f"sales.{table}"
            # key = f"Sales.{table}"
            key = am["key"]

            if match_type == "Full":
                match_attributes = am["attributes"]
                for attrib in match_attributes:
                    schema_attribute = attrib["schema_attribute"]
                    table_attribute = attrib["table_attribute"]
                    attribute_match_type = attrib["match_type"]
                    match_level = attrib.get("match_level")

                    if attribute_match_type == "Full" and match_level == 0:
                        matcher_key = f"{schema}.{schema_attribute}"
                        matcher_value = f"{key}.{table_attribute}"
                        self.matcher[matcher_key] = matcher_value

        # Override user matches over AI matcher
        for umk, umv in self.user_matched.items():
            self.matcher[umk] = f"{umv['key']}.{umv['table_attribute']}"

    def flush_data(self):
        self.matched_row_id = {}
        self.request_body_table_matching = {}
        self.ezapi_data_id_mapping = {}

    def get_ezapi_data_id(self, placeholder_value):
        tmp = placeholder_value.split("._")
        if len(tmp) > 1:
            data_id = tmp[-1]
        else:
            data_id = placeholder_value
        return data_id

    def identify_data_for_get_testcases(self, item, origEzdataid):
        tmp_get_item = {}
        if "dependent-data-id" in item and item["ezapi-data-id"] == origEzdataid:
            self.dependent_data_id = item["dependent-data-id"]
            tmp_get_item = item
        elif self.dependent_data_id == item["ezapi-data-id"]:
            tmp_get_item = item
        return tmp_get_item

    def get_array_schema_ref_data(self, schema_ref, checking_dict):
        print("Entered response body")
        schema_name = None
        if "ref" in schema_ref:
            schema_name = schema_ref["ref"]
        elif "ezapi_ref" in schema_ref:
            schema_name = schema_ref["ezapi_ref"].rsplit("/", 1)[1]

        schema_attributes = self.schemas[schema_name]
        schema_tables = {}
        for s in schema_attributes:
            s_name = s
            if 'type' in schema_attributes[s]:
                s_type = schema_attributes[s]['type']
            elif 'ezapi_ref' in schema_attributes[s]:
                s_type = "ezapi_ref"
            s_lookup_key = f"{schema_name}.{s_name}"

            if s_type in DATA_TYPE_LIST:
                if s_lookup_key in self.matcher:
                    s_matcher = self.matcher[f"{schema_name}.{s_name}"]
                    table_name, column = s_matcher.rsplit(".", 1)
                    if table_name not in schema_tables:
                        schema_tables[table_name] = []
                    schema_tables[table_name].append(column)
                else:
                    ret[s_name] = shortuuid.uuid()
            else:
                if s_type == 'ezapi_ref':
                    tmp_schema = {}
                    tmp_schema[s] = schema_attributes[s]
                    tmp_schema = schema_attributes[s]
                    ret[s_name] = self.get_array_schema_ref_data(tmp_schema)
                elif s_type == 'object' and 'properties' in s:
                    ret[s_name] = self.get_object_data(s)
                elif s_type == 'array':
                    if 'items' in schema_attributes[s]:
                        tmp_result = self.get_body_data(schema_attributes[s]['items'])
                        ret[s_name] = tmp_result

        rets = []
        ret = {}
        final_rets = []
        final_rets_non_get = []
        final_rets_get = []
        functional_obj_item = {}
        for k, v in schema_tables.items():
            table_name = k
            selected_columns = v

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
                            print("origEzdataid:", origEzdataid)
                            valid_item = self.identify_data_for_get_testcases(item, origEzdataid)
                            print("valid_item:", valid_item)
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

            # ret.pop("ezapi-data-id", None)
            # ret.pop("dependent-data-id", None)

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
                        # matched_id = functional_obj_item['ezapi-data-id']
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

    def get_schema_ref_data(self, schema_ref):
        ret = {}
        schema_name = None
        if "ref" in schema_ref:
            schema_name = schema_ref["ref"]
        elif "ezapi_ref" in schema_ref:
            schema_name = schema_ref["ezapi_ref"].rsplit("/", 1)[1]

        schema_attributes = self.schemas[schema_name]
        schema_tables = {}
        for s in schema_attributes:
            s_name = s
            if 'type' in schema_attributes[s]:
                s_type = schema_attributes[s]['type']
            elif 'ezapi_ref' in schema_attributes[s]:
                s_type = "ezapi_ref"
            s_lookup_key = f"{schema_name}.{s_name}"

            if s_type in DATA_TYPE_LIST:
                if s_lookup_key in self.matcher:
                    s_matcher = self.matcher[f"{schema_name}.{s_name}"]
                    table_name, column = s_matcher.rsplit(".", 1)
                    if table_name not in schema_tables:
                        schema_tables[table_name] = []
                    schema_tables[table_name].append(column)
                else:
                    ret[s_name] = shortuuid.uuid()
            else:
                if s_type == 'ezapi_ref':
                    tmp_schema = {}
                    tmp_schema[s] = schema_attributes[s]
                    tmp_schema = schema_attributes[s]
                    ret[s_name] = self.get_schema_ref_data(tmp_schema)
                elif s_type == 'object' and 'properties' in s:
                    ret[s_name] = self.get_object_data(s)
                elif s_type == 'array':
                    if 'items' in schema_attributes[s]:
                        tmp_result = self.get_body_data(schema_attributes[s]['items'])
                        ret[s_name] = tmp_result


        for stk, stv in schema_tables.items():
            table_name = stk
            selected_columns = stv

            if table_name in self.matched_row_id:
                srow = next(item for item in self.functional[table_name] if item["ezapi-data-id"] == self.matched_row_id[table_name])
                matched_id = srow["ezapi-data-id"]
                tmp = {x: srow[x] for x in srow.keys() & selected_columns}
                ret = {**ret, **tmp}
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
                tmp = {x: rrow[x] for x in rrow.keys() & selected_columns}
                ret = {**ret, **tmp}

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
        schema_key = f"{field_ref['schemaName']}.{field_ref['name']}"
        if schema_key not in self.matcher:
            return shortuuid.uuid()
        
        matched_value = self.matcher[schema_key]
        table_name, column_name = matched_value.rsplit(".", 1)


        field_name = column_name
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

        return ret

    def get_object_data(self, object_ref):
        ret = {}
        for k, v in object_ref["properties"].items():
            v_type = v.get("type")

            if v_type == "ezapi_ref":
                ret[k] = self.get_schema_ref_data(v)
            elif v_type == "object" and "properties" in v:
                ret[k] = self.get_object_data(v)
            elif v_type == "array" and "items" in v:
                ret[k] = [self.get_body_data(v["items"])]
            elif v_type in DATA_TYPE_LIST:
                ret[k] = self.get_field_data(v)
        return ret

    def get_body_data(self, body, req_data):
        ret = {}
        checking_dict = {}
        if not body:
            return ret
        if len(req_data['query']) > 0:
            for k, v in req_data['query'].items():
                checking_dict[k] = v

        if len(req_data['path']) > 0:
            for k, v in req_data['path'].items():
                checking_dict[k] = v

        if not body:
            return ret

        body_type = body.get("type")
        isArray = body.get("isArray")

        if (body_type == "ezapi_ref" or "ezapi_ref" in body) and isArray:
            ret = self.get_array_schema_ref_data(body, checking_dict)
            return ret
        elif body_type == "ezapi_ref" or "ezapi_ref" in body:
            ret = self.get_schema_ref_data(body)
        elif body_type == "object" and "properties" in body:
            ret = self.get_object_data(body)
        elif body_type in DATA_TYPE_LIST:
            ret = self.get_field_data(body)

        return ret

    def set_operation_data(self, method, status):
        self.method = method
        self.status = status

    def get_request_params_data(self, params):
        ret = {}
        for param in params:
            for k, v in param.items():
                ret[k] = self.get_field_data(v, is_body = False)
        if ret:
            self.param_data.append(ret)
        return ret

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

    def get_composite_keys(self, table_name):
        DBG = DBGenerator(self.projectid, None, self.db)
        DBG.fetch_table_data()
        composite_array = DBG.fetch_composite_keys(table_name)
        return composite_array

    def get_array_schema_request_body_data(self, schema_ref):
        DBG = DBGenerator(self.projectid, None, self.db)
        DBG.fetch_table_data()
        ret = {}
        rets = []
        gen_array = []
        schema_name = None
        if "ref" in schema_ref:
            schema_name = schema_ref["ref"]
        elif "ezapi_ref" in schema_ref:
            schema_name = schema_ref["ezapi_ref"].rsplit("/", 1)[1]

        schema_attributes = self.schemas[schema_name]
        schema_tables = {}
        for s in schema_attributes:
            s_name = s
            if 'type' in schema_attributes[s]:
                s_type = schema_attributes[s]['type']
            elif 'ezapi_ref' in schema_attributes[s]:
                s_type = "ezapi_ref"
            s_lookup_key = f"{schema_name}.{s_name}"

            if s_type in DATA_TYPE_LIST:
                if s_lookup_key in self.matcher:
                    s_matcher = self.matcher[f"{schema_name}.{s_name}"]
                    table_name, column = s_matcher.rsplit(".", 1)
                    if table_name not in schema_tables:
                        schema_tables[table_name] = []
                    schema_tables[table_name].append(column)
                else:
                    ret[s_name] = shortuuid.uuid()
            else:
                if s_type == 'ezapi_ref':
                    tmp_schema = {}
                    tmp_schema[s] = schema_attributes[s]
                    tmp_schema = schema_attributes[s]
                    ret[s_name] = self.get_array_schema_ref_data(tmp_schema)
                elif s_type == 'object' and 'properties' in s:
                    ret[s_name] = self.get_object_data(s)
                elif s_type == 'array':
                    if 'items' in schema_attributes[s]:
                        tmp_result = self.get_body_data(schema_attributes[s]['items'])
                        ret[s_name] = tmp_result

        for k, v in schema_tables.items():
            table_name = k
            selected_columns = v

        self.composite_keys = self.get_composite_keys(table_name)
        tmp_data_ids = self.get_matched_ids(table_name)
        print("paramdata", self.param_data)
        print("tmp_data_ids", tmp_data_ids)
        for i in range(2):
            gen = DBG.generate_testcase_data(table_name, [x for x in selected_columns])
            for rk, rv in gen.items():
                if table_name in self.placeholders and rk in self.placeholders[table_name]:
                    gen[rk] = "-"
                    if rv == "placeholder" and len(tmp_data_ids) > 0:
                        gen[rk] = f"placeholder_._{table_name}_._{rk}_._{tmp_data_ids[i]}"  # todo
                    # gen[rk] = f"placeholder.{table_name}.{rk}.{matched_id}"   # todo
                elif rk in self.composite_keys:
                    # ret[rk] = ""  # should get data from db_data
                    comp_key_vals_array = self.get_comp_key_values(rk, table_name)
                    if len(comp_key_vals_array) > 0:
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

    def get_request_body_data(self, body):
        ret = {}
        rets = []
        if not body:
            return ret

        body_type = body.get("type")
        isArray = body.get("isArray")

        if body_type == "ezapi_ref" or "ezapi_ref" in body:
            if isArray:
                rets = self.get_array_schema_request_body_data(body)
                return rets
            ret = self.get_schema_ref_data(body)
        elif body_type == "object" and "properties" in body:
            ret = self.get_object_data(body)
        elif body_type in DATA_TYPE_LIST:
            ret = self.get_field_data(body)

        return ret

    def generate_request_data(self, request_data, is_performance = False):
        ret = {
            "path": self.get_request_params_data(request_data["path"]),
            "query": self.get_request_params_data(request_data["query"]),
            "header": self.get_request_params_data(request_data["header"]),
            "form": {},
            "body": self.get_request_body_data(request_data["body"])
        }
        # pprint({
        #     "ret": ret["body"],
        #     "req": request_data["body"]
        # })

        # if not is_performance:
        #     rets = []
        #     for _ in range(10):
        #         # rquery = handle_required(ret["query"], request_data["query"])
        #         rbody = handle_required(ret["body"], request_data["body"])

        #         tmp = {
        #             "path": ret["path"],
        #             "query": ret["query"],
        #             "header": ret["header"],
        #             "form": ret["form"],
        #             "body": rbody
        #         }
        #         if tmp not in rets:
        #             rets.append(tmp)

        #     return rets

        if not is_performance:
            rets = []
            for _ in range(10):
                # rquery = handle_required(ret["query"], request_data["query"])
                if isinstance(ret["body"], list):
                    ret_body = ret["body"][0]
                    rbody_1 = handle_required(ret_body, request_data["body"], self.schema_attr_req)
                    ret_body = ret["body"][1]
                    rbody_2 = handle_required(ret_body, request_data["body"], self.schema_attr_req)

                    tmp = {
                        "path": ret["path"],
                        "query": ret["query"],
                        "header": ret["header"],
                        "form": ret["form"],
                        "body": [rbody_1, rbody_2]
                    }

                elif isinstance(ret["body"], dict):
                    ret_body = ret["body"]

                    rbody = handle_required(ret_body, request_data["body"], self.schema_attr_req)

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

    def generate_response_data(self, resp , req_data):
        ret = None
        content = resp.get("content")
        if content:
            ret = self.get_body_data(content, req_data)

        response = {
            "status": resp["status_code"],
            "content": ret
        }

        return response