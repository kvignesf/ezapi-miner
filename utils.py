from pprint import pprint

import json
import re

import db_manager

import jsonref

import sys
import os

# minimum, maximum, minLength, maxLength, minItems, maxItems ...
OTHER_FIELDS = set(["enum", "default"])


def deref_json(dict_data):
    jsondata = json.dumps(dict_data)
    jsondata = jsonref.loads(jsondata)
    return jsondata


def extract_type_object(param_object, is_json_schema=False):
    if "type" not in param_object and "properties" in param_object:
        param_object["type"] = "object"

    assert param_object["type"] == "object"

    result = {}

    try:
        all_required_fields = []
        if is_json_schema:
            all_required_fields = param_object.get("required", [])

        for key, val in param_object["properties"].items():
            result[key] = {}
            val_type = val.get("type")

            if not val_type:
                if "items" in val:
                    val["type"] = "array"
                    val_type = "array"
                elif "properties" in val:
                    val["type"] = "object"
                    val_type = "object"

            result[key]["type"] = val_type

            if not is_json_schema:
                result[key]["required"] = val.get("required", False)
            else:
                result[key]["required"] = key in all_required_fields

            if val_type == "array":
                result[key]["items"] = extract_type_array(val, is_json_schema)

            elif val_type == "object":
                result[key]["properties"] = extract_type_object(
                    val, is_json_schema)

            else:
                result[key]["format"] = val.get("format")
                result[key]["description"] = val.get("description")

                for f in OTHER_FIELDS:
                    if f in val:
                        result[key][f] = val.get(f)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno, str(e))

    return result


def extract_type_array(param_array, is_json_schema=False):
    if "type" not in param_array and "items" in param_array:
        param_array["type"] = "array"

    assert param_array["type"] == "array"

    result = {}

    try:
        array_items = param_array.get("items")  # required-field
        item_type = array_items.get("type")  # required-field

        if not item_type:
            if "items" in array_items:
                array_items["type"] = "array"
                item_type = "array"
            elif "properties" in array_items:
                array_items["type"] = "object"
                item_type = "object"

        result["type"] = item_type

        if item_type == "array":
            result["items"] = extract_type_array(array_items, is_json_schema)

        elif item_type == "object":
            result["properties"] = extract_type_object(
                array_items, is_json_schema)

        else:
            result["format"] = array_items.get("format")
            result["description"] = array_items.get("description")

            for f in OTHER_FIELDS:
                if f in array_items:
                    result[f] = array_items.get(f)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno, str(e))

    return result


# def camel_case_split(word):
#     try:
#         word = word[0].upper() + word[1:]
#         res = re.findall(r'[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))', word)
#         print(res)
#         return (" ".join(res)).lower()
#     except:
#         return None


def camel_case_split(identifier):
    try:
        matches = re.finditer(
            '.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
        res = [m.group(0) for m in matches]
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


def get_all_tags(api_ops_id):
    client, db = db_manager.get_db_connection()
    apiinfo = db.apiinfo.find_one({"api_ops_id": api_ops_id})
    tags = apiinfo["tags"]

    if not tags:
        tags = []
    else:
        tags = [t["name"] for t in tags]

    return tags


def get_tags_from_paths(api_ops_id):
    client, db = db_manager.get_db_connection()
    tags = set()
    all_paths = db.paths.find({"api_ops_id": api_ops_id})
    all_paths = list(all_paths)

    for p in all_paths:
        method_def = p.get("method_definition", [])
        for m in method_def:
            if "tags" in m:
                m_tags = m["tags"]
                if m_tags:
                    for t in m_tags:
                        tags.add(t)

    return list(tags)


def special_character_split(operationId):
    res = re.split(r"[^a-zA-Z0-9]", operationId)
    return res


def possibleMatch(s1, s2):  # match str1, str2
    if s1 == s2:
        return 1

    t1 = special_character_split(s1)
    t2 = special_character_split(s2)

    s1 = split_operation_id(s1)
    s2 = split_operation_id(s2)

    s1 = s1.split()
    s2 = s2.split()

    if t1 == t2 or s1 == t2 or s2 == t1:    # all words matched
        return 1

    # for t1 in s1:
    #     for t2 in s2:
    #         if t1 == t2:    # some words matched
    #             return 2

    return 0


# JFT - Petstore
"""
T = ["uploadFile", "addPet", "updatePet", "findPetsByStatus", "findPetsByTags", "getPetById", "updatePetWithForm", "deletePet", "placeOrder", "getOrderById", "deleteOrder",
     "getInventory", "createUsersWithArrayInput", "createUsersWithListInput", "getUserByName", "updateUser", "deleteUser", "loginUser", "logoutUser", "createUser"]

for t in T:
    print(t, split_operation_id(t))
"""
