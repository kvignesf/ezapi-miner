# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************


from io import StringIO
from pprint import pprint
import re


def convert_to_camel_case(s):
    res = re.split(r"[^a-zA-Z0-9]", s)
    res = [x.lower() for x in res]
    res = "".join(x.title() for x in res)
    return res


def get_jdl_type(openapi_tf):
    jdl_type = None

    otype = openapi_tf.get("type", "string")
    oformat = openapi_tf.get("format", None)

    if otype == "string":
        if oformat == "date":
            jdl_type = "LocalDate"
        elif oformat == "date-time":
            jdl_type = "ZonedDateTime"
        else:
            jdl_type = "String"
    elif otype == "boolean":
        jdl_type = "Boolean"
    elif otype == "integer":
        if oformat == "int32":
            jdl_type = "String"  # quick fix
        elif oformat == "int64":
            jdl_type = "String"  # quick fix
        else:
            jdl_type = "Long"
    elif otype == "number":
        if oformat == "float":
            jdl_type = "Float"
        elif oformat == "double":
            jdl_type = "Double"
        else:
            jdl_type = "Double"
    else:
        jdl_type = "String"

    return jdl_type


"""
claim.claim_status -> {
    tabel: tbl_clm_claim_master
    column: claim-status
}
"""


def transform_matcher(matcher_data):
    matcher_dict = {}

    for md in matcher_data:
        schema_name = md["schema"]
        table_name = md["table"]

        # todo - parent name
        for attr in md["attributes"]:
            if attr.get("match_type") == "Full" and attr.get("match_level") == 0:
                schema_attribute = attr["schema_attribute"]
                table_attribute = attr["table_attribute"]

                dict_key = schema_name + "." + schema_attribute
                matcher_dict[dict_key] = {
                    "table": table_name,
                    "column": table_attribute,
                }

    return matcher_dict


def extract_dtype_id(entity_tables, table_data):
    table_dict = {}

    for t in table_data:
        table_dict[t["table"]] = {}
        for t2 in t["attributes"]:
            table_dict[t["table"]][t2["name"]] = t2

    ret = {}
    for entity, entity_attr in entity_tables.items():
        if entity in table_dict:
            ret[entity] = {
                "custom_name": convert_to_camel_case(entity),
                "columns": {},
            }

            # quick fix
            if ret[entity]["custom_name"] and ret[entity]["custom_name"].endswith(
                "Detail"
            ):
                ret[entity]["custom_name"] += "s"

            for attr in entity_attr:
                if attr in table_dict[entity]:
                    column_data = table_dict[entity][attr]
                    ret[entity]["columns"][attr] = {
                        "openapi": column_data.get("openapi"),
                        "type": column_data.get("datatype"),
                        "key": column_data.get("key") == "primary",
                        "custom_name": convert_to_camel_case(attr),
                        "jdl_type": get_jdl_type(column_data.get("openapi")),
                    }
    return ret


class ExtractTableEntity:
    def __init__(self):
        self.entity_tables = {}

    def extract_field_data(self, data):
        name = data["name"]
        source_name = data.get("sourceName", name)

        matched_table = None
        matched_column = None

        if "tableName" in data:
            matched_table = data["tableName"]
            matched_column = source_name

        if matched_table and matched_column:
            if matched_table not in self.entity_tables:
                self.entity_tables[matched_table] = set()
            self.entity_tables[matched_table].add(matched_column)

    def extract_table_data(self, data):
        name = data["name"]
        source_name = data.get("sourceName", name)
        selected_columns = data.get("selectedColumns", None)

        if selected_columns:
            if source_name not in self.entity_tables:
                self.entity_tables[source_name] = set()
            for s in selected_columns:
                column_name = s["name"]
                column_source_name = s.get("sourceName", column_name)
                self.entity_tables[source_name].add(column_source_name)

    def extract_object_data(self, data):
        for k, v in data["properties"].items():
            vt = v.get("type", None)
            if vt == "ezapi_table":
                self.extract_table_data(v)
            elif vt == "object":
                self.extract_object_data(v)
            elif vt == "array":
                self.extract_array_data(v)
            elif vt in ["string", "number", "integer", "boolean"]:
                self.extract_field_data(v)

    def extract_array_data(self, data):
        pass

    def extract_body_data(self, data):  # request body / response content
        body_type = data.get("type")
        if body_type == "object":
            self.extract_object_data(data)
        elif body_type == "ezapi_table":
            self.extract_table_data(data)
        elif body_type in ["string", "number", "integer", "boolean"]:
            self.extract_field_data(data)


class ExtractSchemaEntity:
    def __init__(self, matcher_dict, schemas):
        self.entity_tables = {}
        self.matcher_dict = matcher_dict
        self.schemas = schemas

    def extract_field_data(self, data, parent=None):
        matched_table = None
        matched_column = None

        # drag n drop attribute
        if "schemaName" in data:
            name = data["name"]
            source_name = data.get("sourceName", name)
            parent_name = data.get("parentName", None)

            schema_name = data["schemaName"]
            matched_key = schema_name + "." + source_name
            if matched_key in self.matcher_dict:
                matched_table = self.matcher_dict[matched_key]["table"]
                matched_column = self.matcher_dict[matched_key]["column"]

        # schema attribute
        elif parent:
            parent = parent.split(".")  # schema.attribute1.attribute2...
            if len(parent) == 2:  # level 1 only
                schema_name = parent[0]
                source_name = parent[1]
                matched_key = schema_name + "." + source_name
                if matched_key in self.matcher_dict:
                    matched_table = self.matcher_dict[matched_key]["table"]
                    matched_column = self.matcher_dict[matched_key]["column"]

        if matched_table and matched_column:
            if matched_table not in self.entity_tables:
                self.entity_tables[matched_table] = set()
            self.entity_tables[matched_table].add(matched_column)

    def extract_object_data(self, data, parent=None):
        for k, v in data["properties"].items():
            new_parent = ((parent or ".") + "." + k).strip(".")
            vt = v.get("type", None)
            if vt == "ezapi_ref":
                self.extract_schema_data(v)
            elif vt == "object":
                self.extract_object_data(v, new_parent)
            elif vt == "array":
                self.extract_array_data(v, new_parent)
            elif vt in ["string", "number", "integer", "boolean"]:
                self.extract_field_data(v, new_parent)

    def extract_array_data(self, data, parent=None):
        pass

    def extract_schema_data(self, data):
        schema_name = data.get("ezapi_ref", None)
        schema_name = schema_name.split("/")[-1]
        parent = schema_name

        if schema_name in self.schemas:
            schema_data = self.schemas[schema_name]
            schema_type = schema_data.get("type", None)

            if schema_type == "object":
                self.extract_object_data(schema_data, parent)
            elif schema_type in ["string", "number", "integer", "boolean"]:
                self.extract_field_data(schema_data, parent)
            elif schema_type == "array":
                self.extract_array_data(schema_data, parent)

    def extract_body_data(self, data):
        body_type = data.get("type")
        if body_type == "object":
            self.extract_object_data(data)
        elif body_type == "ezapi_ref":
            self.extract_schema_data(data)
        elif body_type in ["string", "number", "integer", "boolean"]:
            self.extract_field_data(data)


def extract_entity_tables(
    project_type, operation_data, schemas_data, matcher_data, table_data
):
    if project_type == "db":
        EntityObj = ExtractTableEntity()
    elif project_type == "both":
        matcher_dict = transform_matcher(matcher_data)
        EntityObj = ExtractSchemaEntity(matcher_dict=matcher_dict, schemas=schemas_data)
    else:
        return None

    for od in operation_data:
        x = od["data"]
        request_data = x["requestData"]
        response_data = x["responseData"]

        parameter_data = (
            request_data["path"] + request_data["query"] + request_data["header"]
        )
        body_data = request_data.get("body", {})

        # parameters
        for param in parameter_data:
            for k, v in param.items():
                EntityObj.extract_field_data(v)

        # request body
        if body_data:
            EntityObj.extract_body_data(body_data)

        # response
        for resp in response_data:
            resp_headers = resp.get("headers", [])
            resp_content = resp.get("content", {})

            for rh in resp_headers:
                for _, rhv in rh.items():
                    EntityObj.extract_field_data(rhv)

            if resp_content:
                EntityObj.extract_body_data(resp_content)

    entity_tables = EntityObj.entity_tables
    entity_tables = extract_dtype_id(entity_tables, table_data)
    return entity_tables