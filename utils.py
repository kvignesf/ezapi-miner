import json
import jsonref

from pprint import pprint


def deref_json(dict_data):
    jsondata = json.dumps(dict_data)
    jsondata = jsonref.loads(jsondata)
    return jsondata


def extract_type_array(param_array, is_json_schema=False):
    assert param_array["type"] == "array"

    result = {}

    array_items = param_array.get("items")  # required-field
    item_type = array_items.get("type")  # required-field

    result["type"] = item_type

    if item_type == "array":
        result["items"] = extract_type_array(array_items, is_json_schema)

    elif item_type == "object":
        result["properties"] = extract_type_object(array_items, is_json_schema)

    else:
        result["format"] = array_items.get("format")
        result["description"] = array_items.get("description")

    return result


def extract_type_object(param_object, is_json_schema=False):
    assert param_object["type"] == "object"

    result = {}

    all_required_fields = []
    if is_json_schema:
        all_required_fields = param_object.get("required", [])

    for key, val in param_object["properties"].items():
        result[key] = {}
        result[key]["type"] = val["type"]

        if not is_json_schema:
            result[key]["required"] = val.get("required", False)
        else:
            result[key]["required"] = key in all_required_fields

        if val["type"] == "array":
            result[key]["items"] = extract_type_array(val, is_json_schema)

        elif val["type"] == "object":
            result[key]["properties"] = extract_type_object(val, is_json_schema)

        else:
            result[key]["format"] = val.get("format")
            result[key]["description"] = val.get("description")

    return result
