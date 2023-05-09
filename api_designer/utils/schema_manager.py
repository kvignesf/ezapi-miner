# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************


from api_designer.utils.common import *
from pprint import pprint

# def get_attribute_critical_score(attributes, schema_name):
#     attribute_vocab = {}

#     for at in attributes:
#         at_split = string_split(at["name"])
#         for word in at_split:
#             if word not in attribute_vocab:
#                 attribute_vocab[word] = 0
#             attribute_vocab[word] += 1

#     for idx, at in enumerate(attributes):
#         at_score = 0
#         at_split = string_split(at["name"])
#         for word in at_split:
#             at_score += attribute_vocab[word]

#         at_score = round(at_score / len(at_split), 2)

#         attributes[idx]["score"] = at_score

#     return attributes


class SchemaSize:
    def __init__(self, schemas):
        self.schemas = schemas  # All schemas

    def get_array_size(self, param_array, visited_schema_list):  # dereferencing array
        assert param_array["type"] == "array"
        res = 0
        depth = 0

        array_items = param_array.get("items")
        array_items_type = array_items.get("type")

        if "ezapi_ref" in array_items:
            ref_schema = array_items["ezapi_ref"].split("/")[-1]
            ref_schema_name = ref_schema
            ref_schema = self.schemas[ref_schema]
            if ref_schema_name not in visited_schema_list:
                visited_schema_list.append(ref_schema_name)
                tmp = self.get_schema_size(ref_schema, visited_schema_list)
                visited_schema_list.remove(ref_schema_name)
                res += tmp[0]
                depth = max(depth, 1 + tmp[1])

        else:
            if array_items_type == "array":
                tmp = self.get_array_size(array_items, visited_schema_list)
                res += tmp[0]
                depth = max(depth, tmp[1])

            elif array_items_type == "object":
                tmp = self.get_object_size(array_items, visited_schema_list)
                res += tmp[0]
                depth = max(depth, 1 + tmp[1])

            else:
                print("enteredHere")
                res += 1
                depth = max(depth, 1)

        return res, depth

    def get_object_size(self, param_object, visited_schema_list):  # dereferencing object
        assert param_object["type"] == "object"
        res = 0
        depth = 0

        if "properties" in param_object:
            for key, value in param_object["properties"].items():
                value_type = value.get("type")

                if "ezapi_ref" in value:
                    ref_schema = value["ezapi_ref"].split("/")[-1]
                    ref_schema_name = ref_schema
                    ref_schema = self.schemas[ref_schema]
                    if ref_schema_name not in visited_schema_list:
                        visited_schema_list.append(ref_schema_name)
                        tmp = self.get_schema_size(ref_schema, visited_schema_list)
                        visited_schema_list.remove(ref_schema_name)
                        res += tmp[0]
                        depth = max(depth, 1 + tmp[1])

                else:
                    if value_type == "object":
                        tmp = self.get_object_size(value, visited_schema_list)
                        res += tmp[0]
                        depth = max(depth, 1 + tmp[1])

                    elif value_type == "array":
                        tmp = self.get_array_size(value, visited_schema_list)
                        res += tmp[0]
                        depth = max(depth, tmp[1])

                    else:
                        res += 1
                        depth = max(depth, 1)

        return res, depth

    def get_schema_size(self, param_schema, visited_schema_list):  # dereferencing schema
        res = 0
        depth = 0

        if "allOf" in param_schema:
            all_schema = param_schema["allOf"]
            for s in all_schema:
                print("s:", s, "\n")
                tmp = self.get_schema_size(s, visited_schema_list)
                res += tmp[0]
                depth = max(depth, tmp[1])

        else:
            st = param_schema.get("type")
            if "ezapi_ref" in param_schema:
                ref_schema = param_schema["ezapi_ref"].split("/")[-1]
                ref_schema_name = ref_schema
                ref_schema = self.schemas[ref_schema]
                print("ref_schema:", ref_schema, "\n")
                if ref_schema_name not in visited_schema_list:
                    visited_schema_list.append(ref_schema_name)
                    tmp = self.get_schema_size(ref_schema, visited_schema_list)
                    visited_schema_list.remove(ref_schema_name)
                    res += tmp[0]
                    depth = max(depth, tmp[1])

            elif st == "object":
                print("obj_param_schema: ", param_schema, "\n")
                tmp = self.get_object_size(param_schema, visited_schema_list)
                res += tmp[0]
                depth = max(depth, tmp[1])

            elif st == "array":
                print("array_param_schema: ", param_schema, "\n")
                tmp = self.get_array_size(param_schema, visited_schema_list)
                res += tmp[0]
                depth = max(depth, tmp[1])

            else:
                res += 1
                depth = 1

        return res, depth


class SchemaDeref:
    def __init__(self, schemas):
        self.schemas = schemas  # All Schema

    def deref_array(self, param_array):
        assert param_array["type"] == "array"
        res = {}

        array_items = param_array.get("items")
        array_items_type = array_items.get("type")

        if "ezapi_ref" in array_items:
            ref_schema = array_items["ezapi_ref"].split("/")[-1]
            ref_schema = self.schemas[ref_schema]
            res = self.deref_schema(ref_schema)

        else:
            res["type"] = array_items_type
            if array_items_type == "array":
                res["items"] = self.deref_array(array_items)

            elif array_items_type == "object":
                res["properties"] = self.deref_object(array_items)

        return res

    def deref_object(self, param_object):
        assert param_object["type"] == "object"
        res = {}

        if "properties" in param_object:  # only additionalProperties present
            for key, value in param_object["properties"].items():
                value_type = value.get("type")

                res[key] = {}

                if "ezapi_ref" in value:
                    ref_schema = value["ezapi_ref"].split("/")[-1]
                    ref_schema = self.schemas[ref_schema]
                    res[key] = self.deref_schema(ref_schema)

                else:
                    res[key]["type"] = value_type

                    if value_type == "object":
                        res[key]["properties"] = self.deref_object(value)
                    elif value_type == "array":
                        res[key]["items"] = self.deref_array(value)

        return res

    def deref_schema(self, param_schema):
        res = {}

        if "allOf" in param_schema:
            all_schemas = param_schema["allOf"]
            res["type"] = "object"
            res["properties"] = {}

            for s in all_schemas:
                s_res = self.deref_schema(s)
                if "properties" in s_res:
                    res["properties"].update(s_res["properties"])

        else:
            st = param_schema.get("type")
            if "ezapi_ref" in param_schema:
                ref_schema = param_schema["ezapi_ref"].split("/")[-1]
                ref_schema = self.schemas[ref_schema]
                res = self.deref_schema(ref_schema)

            elif st == "object":
                res["type"] = "object"
                res["properties"] = self.deref_object(param_schema)

            elif st == "array":
                res["type"] = "array"
                res["items"] = self.deref_array(param_schema)

            else:
                res["type"] = st

        return res


class SchemaCrawler:
    def __init__(self, schema_name):
        self.schema_name = schema_name
        self.elements = []

    def extract_schema_array(self, param_array, level=0, parent=None, prev_key=None):
        elems = []

        pt = param_array.get("type")

        if not pt and "ezapi_ref" in param_array:
            pt = "ezapi_ref"

        if pt and pt not in ("array", "object", "ezapi_ref"):
            tmp = {
                "name": prev_key,
                "type": pt,
                "format": param_array.get("format"),
                "required": param_array.get("required"),
                "level": level,
                "parent": parent,
                "is_child": True,
            }
            elems.append(tmp)

        elif pt == "object" and "properties" in param_array:
            tmp = self.extract_schema_object(param_array, level, parent)
            if tmp:
                elems += tmp

        elif pt == "ezapi_ref":
            ref = param_array["ezapi_ref"].split("/")[-1]
            refPath = param_array["ezapi_ref"]

            tmp = {
                "name": prev_key,
                "type": pt,
                "level": level,
                "parent": parent,
                "is_child": False,
                "ref": ref,
                "refPath": refPath,
            }
            elems.append(tmp)

        return elems

    def extract_schema_object(self, param_object, level=0, parent=None):
        elems = []

        if "properties" in param_object:
            for k, v in param_object["properties"].items():
                v_type = v.get("type")

                if not v_type and "ezapi_ref" in v:
                    v_type = "ezapi_ref"

                if v_type:
                    if v_type == "ezapi_ref":
                        ref = v["ezapi_ref"].split("/")[-1]
                        refPath = v["ezapi_ref"]

                        tmp = {
                            "name": k,
                            "type": v_type,
                            "level": level,
                            "parent": parent,
                            "is_child": False,
                            "ref": ref,
                            "refPath": refPath,
                        }
                        elems.append(tmp)

                    elif v_type not in ("array", "object", "ezapi_ref"):
                        tmp = {
                            "name": k,
                            "type": v_type,
                            "format": v.get("format"),
                            "required": v.get("required"),
                            "level": level,
                            "parent": parent,
                            "is_child": True,
                        }
                        elems.append(tmp)

                    elif v_type == "object" and "properties" in v:
                        tmp = {
                            "name": k,
                            "type": v_type,
                            "format": v.get("format"),
                            "required": v.get("required"),
                            "level": level,
                            "parent": parent,
                            "is_child": False,
                        }
                        elems.append(tmp)

                        new_par = ((parent or "") + "." + k).strip(".")
                        res = self.extract_schema_object(v, level + 1, new_par)
                        if res:
                            elems += res

                    elif v_type == "array" and "items" in v:
                        further = True
                        further_type = None

                        if "type" in v["items"] and v["items"]["type"] not in (
                            "object",
                            "array",
                        ):
                            further = False
                            further_type = v["items"]["type"]

                        tmp = {
                            "name": k,
                            "type": v_type,
                            "format": v.get("format"),
                            "required": v.get("required"),
                            "level": level,
                            "parent": parent,
                            "is_child": not further,
                            "sub_type": further_type,
                        }
                        elems.append(tmp)

                        if further:
                            new_par = ((parent or "") + "." + k).strip(".")
                            res = self.extract_schema_array(
                                v["items"], level + 1, new_par, k
                            )
                            if res:
                                elems += res

            return elems

    def extract_schema_attrs(self, param_schema):
        if "allOf" in param_schema:
            all_schemas = param_schema["allOf"]

            for s in all_schemas:
                self.extract_schema_attrs(s)

        else:
            st = param_schema.get("type")
            if st == "object" and "properties" in param_schema:
                res = self.extract_schema_object(param_schema)

                self.elements += res

        return self.elements


def crawl_schema(schemas):
    crawled_schema = []
    ss = SchemaSize(schemas)
    visited_schema_list = []

    for k, v in schemas.items():
        print("schemaName:", k)
        visited_schema_list.append(k)
        schema_name = k
        schema_description = v.get("description")

        cs = SchemaCrawler(schema_name)
        elements = cs.extract_schema_attrs(v)

        original_elements = [
            x for x in elements if x["type"] != "ezapi_ref" and x["level"] == 0
        ]

        if len(original_elements) > 0:  # filter out combination of other schemas
            schema_size, schema_depth = ss.get_schema_size(v, visited_schema_list)

            crawled_schema.append(
                {
                    "name": schema_name,
                    "description": schema_description,
                    "size": schema_size,
                    "max_depth": schema_depth,
                    "attributes": elements,
                }
            )
            visited_schema_list = []
    return crawled_schema
