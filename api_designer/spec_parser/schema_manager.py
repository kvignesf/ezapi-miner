# ----------- Begin of util code -----------
import re


def is_camel_case(s):
    return s != s.lower() and s != s.upper() and "_" not in s


def is_under_score(s):
    return "_" in s


def camel_case_split(s):
    try:
        matches = re.finditer(
            ".+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)", s
        )
        res = [m.group(0) for m in matches]
        res = [x.lower() for x in res]
        return res
    except:
        return [s]


def special_character_split(s):
    res = re.split(r"[^a-zA-Z0-9]", s)
    res = [x.lower() for x in res]
    return res


def string_split(s):
    if is_camel_case(s):
        return camel_case_split(s)
    return special_character_split(s)


# ----------- End of util code -----------


def check_element(element):
    element_type = element.get("type")
    return element_type and element_type not in ("array", "object")


def extract_schema_attributes(param_schema, all_off_schema=False):
    try:
        elements = []
        refs = []

        if "allOf" in param_schema:
            all_schemas = param_schema["allOf"]

            for s in all_schemas:
                tmp = extract_schema_attributes(s, True)
                elements += tmp[0]
                refs += tmp[1]

        else:
            st = param_schema.get("type")

            # Check Petstore Pet Schema
            all_required_fields = []
            all_required_fields = param_schema.get("required", [])
            if all_required_fields:
                print("all required - ", all_required_fields)

            if st == "object" and "properties" in param_schema:
                for key, value in param_schema["properties"].items():
                    if check_element(value):
                        elem_type = value.get("type")
                        elem_format = value.get("format")
                        elem_required = value.get("required", False)

                        if key in all_required_fields:
                            elem_required = True

                        elements.append(
                            {
                                "name": key,
                                "type": elem_type,
                                "format": elem_format,
                                "required": elem_required,
                                "all_of_schema": all_off_schema,
                            }
                        )
                    else:
                        elem_type = value.get("type")
                        elem_format = value.get("format")
                        elem_required = value.get("required", False)

                        if key in all_required_fields:
                            elem_required = True

                        refs.append(
                            {
                                "name": key,
                                "type": elem_type,
                                "format": elem_format,
                                "required": elem_required,
                                "all_of_schema": all_off_schema,
                            }
                        )
                    # if "ezapi_ref" in value:
                    #     refs.append(key)

    except Exception as e:
        print("**Error", param_schema)
        print(str(e))

    return elements, refs


def get_attribute_critical_score(attributes, schema_name):
    attribute_vocab = {}

    for at in attributes:
        at_split = string_split(at["name"])
        for word in at_split:
            if word not in attribute_vocab:
                attribute_vocab[word] = 0
            attribute_vocab[word] += 1

    for idx, at in enumerate(attributes):
        at_score = 0
        at_split = string_split(at["name"])
        for word in at_split:
            at_score += attribute_vocab[word]

        at_score = round(at_score / len(at_split), 2)

        attributes[idx]["score"] = at_score

    return attributes


class SchemaSize:
    def __init__(self, schemas):
        self.schemas = schemas  # All schemas

    def get_array_size(self, param_array):  # dereferencing array
        assert param_array["type"] == "array"
        res = 0
        depth = 0

        array_items = param_array.get("items")
        array_items_type = array_items.get("type")

        if "ezapi_ref" in array_items:
            ref_schema = array_items["ezapi_ref"].split("/")[-1]
            ref_schema = self.schemas[ref_schema]
            tmp = self.get_schema_size(ref_schema)
            res += tmp[0]
            depth = max(depth, 1 + tmp[1])

        else:
            if array_items_type == "array":
                tmp = self.get_array_size(array_items)
                res += tmp[0]
                depth = max(depth, tmp[1])

            elif array_items_type == "object":
                tmp = self.get_object_size(array_items)
                res += tmp[0]
                depth = max(depth, 1 + tmp[1])

            else:
                res += 1
                depth = max(depth, 1)

        return res, depth

    def get_object_size(self, param_object):  # dereferencing object
        assert param_object["type"] == "object"
        res = 0
        depth = 0

        if "properties" in param_object:
            for key, value in param_object["properties"].items():
                value_type = value.get("type")

                if "ezapi_ref" in value:
                    ref_schema = value["ezapi_ref"].split("/")[-1]
                    ref_schema = self.schemas[ref_schema]
                    tmp = self.get_schema_size(ref_schema)
                    res += tmp[0]
                    depth = max(depth, 1 + tmp[1])

                else:
                    if value_type == "object":
                        tmp = self.get_object_size(value)
                        res += tmp[0]
                        depth = max(depth, 1 + tmp[1])

                    elif value_type == "array":
                        tmp = self.get_array_size(value)
                        res += tmp[0]
                        depth = max(depth, tmp[1])

                    else:
                        res += 1
                        depth = max(depth, 1)

        return res, depth

    def get_schema_size(self, param_schema):  # dereferencing schema
        res = 0
        depth = 0

        if "allOf" in param_schema:
            all_schema = param_schema["allOf"]
            for s in all_schema:
                tmp = self.get_schema_size(s)
                res += tmp[0]
                depth = max(depth, tmp[1])

        else:
            st = param_schema.get("type")
            if "ezapi_ref" in param_schema:
                ref_schema = param_schema["ezapi_ref"].split("/")[-1]
                ref_schema = self.schemas[ref_schema]
                tmp = self.get_schema_size(ref_schema)
                res += tmp[0]
                depth = max(depth, tmp[1])

            elif st == "object":
                tmp = self.get_object_size(param_schema)
                res += tmp[0]
                depth = max(depth, tmp[1])

            elif st == "array":
                tmp = self.get_array_size(param_schema)
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


def crawl_schema(schemas):
    crawled_schema = []
    ss = SchemaSize(schemas)

    for k, v in schemas.items():
        desc = v.get("description")
        attrib, refs = extract_schema_attributes(v)

        attrib = get_attribute_critical_score(attrib, k)

        if len(attrib) > 0:  # filter out combination of other schemas
            size, depth = ss.get_schema_size(v)
            crawled_schema.append(
                {
                    "name": k,
                    "description": desc,
                    "size": size,
                    "max_depth": depth,
                    "attributes": attrib,
                    "refs": refs,
                }
            )

    return crawled_schema
