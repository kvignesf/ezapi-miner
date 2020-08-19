from pprint import pprint

import json
import re

import jsonref


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
            result[key]["properties"] = extract_type_object(
                val, is_json_schema)

        else:
            result[key]["format"] = val.get("format")
            result[key]["description"] = val.get("description")

    return result


def camel_case_split(word):
    try:
        word = word[0].upper() + word[1:]
        res = re.findall(r'[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))', word)
        return (" ".join(res)).lower()
    except:
        return None


# Reference - https://github.com/TimKam/compound-word-splitter
# enchant installed required - https://abiword.github.io/enchant/
def word_split(word):
    try:
        import splitter
        word = word.lower()
        res = splitter.split(word)
        if "".join(res) == word:
            return " ".join(res)
        return None
    except:
        return None


def split_operation_id(operationId):
    res1 = camel_case_split(operationId)
    res2 = word_split(operationId)

    if res1 and res2:
        if res1 == res2:
            return res1
        elif res1.lower() == operationId.lower():
            return res2
        else:
            return res1
    elif res1:
        return res1
    elif res2:
        return res2
    else:
        return operationId


def extract_apiops_description(operationId, description, summary):
    if operationId:
        return split_operation_id(operationId)
    elif description or summary:
        if not description:
            return summary
        elif not summary:
            return description
        else:
            if len(description) < len(summary):
                return description
            else:
                return summary
    return None


# JFT - Petstore
"""
T = ["uploadFile", "addPet", "updatePet", "findPetsByStatus", "findPetsByTags", "getPetById", "updatePetWithForm", "deletePet", "placeOrder", "getOrderById", "deleteOrder",
     "getInventory", "createUsersWithArrayInput", "createUsersWithListInput", "getUserByName", "updateUser", "deleteUser", "loginUser", "logoutUser", "createUser"]

for t in T:
    print(t, split_operation_id(t))
"""
