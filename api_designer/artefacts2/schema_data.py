from pprint import pprint
import random
from numpy import isin
import shortuuid

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

def handle_required(data, source):
    # data_keys = list(get_all_keys(data))
    required_info = list(get_all_required_info(source))

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
        schemas = self.db.components.find_one({"projectid": self.projectid})
        schemas = schemas["data"]["schemas"]
        for sk, sv in schemas.items():
            schema_name = sk
            if "type" in sv and sv["type"] == "object" and "properties" in sv:
                schema_attributes = sv["properties"]
                self.schemas[schema_name] = schema_attributes


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

    def get_body_data(self, body):
        ret = {}
        if not body:
            return ret

        body_type = body.get("type")
        if body_type == "ezapi_ref" or "ezapi_ref" in body:
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
        return ret

    def generate_request_data(self, request_data, is_performance = False):
        ret = {
            "path": self.get_request_params_data(request_data["path"]),
            "query": self.get_request_params_data(request_data["query"]),
            "header": self.get_request_params_data(request_data["header"]),
            "form": {},
            "body": self.get_body_data(request_data["body"])
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
        return ret

    def generate_response_data(self, resp):
        ret = None
        content = resp.get("content")
        if content:
            ret = self.get_body_data(content)

        response = {
            "status": resp["status_code"],
            "content": ret
        }

        return response