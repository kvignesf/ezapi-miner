# from api_designer import config
from pprint import pprint
import re

constraint = re.compile("\[(\w+)\] +(asc|desc)")

# Reference - https://www.w3schools.com/sql/sql_datatypes.asp
_DATA_TYPES_NUMERIC = [
    "bit",
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "decimal",
    "numeric",
    "smallmoney",
    "money",
    "float",
    "real",
]
_DATA_TYPES_DATETIME = [
    "datetime",
    "datetime2",
    "smalldatetime",
    "date",
    "time",
    "datetimeoffset",
    "timestamp",
]
_DATA_TYPES_STRING = [
    "char",
    "varchar",
    "varchar",
    "text",
    "nchar",
    "nvarchar",
    "nvarchar",
    "ntext",
    "binary",
    "varbinary",
    "varbinary",
    "image",
]
_DATA_TYPES_OTHER = ["sql_variant", "uniqueidentifier", "xml", "cursor", "table"]

_DATA_TYPES_ALL = (
    _DATA_TYPES_NUMERIC + _DATA_TYPES_DATETIME + _DATA_TYPES_STRING + _DATA_TYPES_OTHER
)


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        text = text[len(prefix) :]
        text = text.strip()
    return text


def extract_table_data(text):
    print("Extracting table data - ", text)
    schema_name = None
    table_name = None

    text = remove_prefix(text, "create table")
    text = text.split(".")  # separate schema and table

    if len(text) == 1:
        table_name = re.sub("[\[\]]", "", text[0])

    elif len(text) == 2:
        schema_name = re.sub("[\[\]]", "", text[0])
        table_name = re.sub("[\[\]]", "", text[1])

    return schema_name, table_name


def extract_column_values(text):
    text = re.sub("[\[\]]", "", text)
    text = text.strip(" ,")
    text = text.split(" ")

    column_values = {}

    if len(text) >= 2:
        name, dt = text[0], text[1]

        if dt.split("(")[0] not in _DATA_TYPES_ALL:
            print("*Error - Unidentified Data Type ", dt)
            return None

        column_values["name"] = name
        column_values["datatype"] = dt
        column_values["valueconstraint"] = "null"

        if "identity(1,1)" in text:
            column_values["serial"] = True

        if len(text) >= 3 and text[-1] == "not":
            column_values["valueconstraint"] = "not null"

        return column_values
    return None


def extract_constraint_values(text):
    rets = constraint.findall(text)
    rets = [x[0] for x in rets]
    return rets


def extract_column_data(text):
    column_values = []
    constraint_keys = None
    is_composite_key = False

    # separate column data and constraints
    tmp = text.split("constraint ", 1)

    if len(tmp) == 1:
        column_data = tmp[0]  # look for "on primary"
    elif len(tmp) == 2:
        column_data = tmp[0]
        contraint_data = tmp[1]

        constraint_keys = extract_constraint_values(contraint_data)
        if len(constraint_keys) > 1:
            is_composite_key = True

    column_data = column_data.strip(" ,")  # multiple characters
    column_data = column_data.split(" null")  # split on null or not null
    column_data = list(filter(None, column_data))

    for cc in column_data:
        ret = extract_column_values(cc)
        if ret:
            if constraint_keys and ret["name"] in constraint_keys:
                ret["key"] = "composite" if is_composite_key else "primary"
            column_values.append(ret)

    return column_values


def extract_default_data(text):
    text = text.split(" for ")

    if len(text) != 2:
        return None

    default_val = text[0]
    default_col = text[1]

    return None


def get_alter_table_data(text):
    key = None
    type = None

    if " foreign key " in text:  # foreign key
        type = "foreign"
        pass

    elif " default " in text and " add constraint " in text:  # default value
        type = "default"

        table_data = text.split("add constraint")[0].split("alter table")[1]
        default_data = text.split(" default ")[1]

        schema_name, table_name = extract_table_data(table_data)
        default_val, default_col = extract_default_data(default_data)

    # return {"type": type, "key": key}
    return None


def get_table_data(lines):
    res = []
    for line in lines:
        line = line.replace("\n", " ")
        line = re.sub("\t", " ", line)
        line = re.sub(" +", " ", line)
        line = line.strip()

        if line.startswith("create table"):  # table found
            tmp = line.split("(", 1)

            if len(tmp) != 2:
                continue

            table_data = extract_table_data(tmp[0])
            column_data = extract_column_data(tmp[1])

            res.append(
                {
                    "schema": table_data[0],
                    "table": table_data[1],
                    "attributes": column_data,
                }
            )

        # if line.startswith("alter table"):
        #     get_alter_table_data(line)

    return res


def parse_ddl_file(ddl_file, api_design_id=None, ddl_filename=None, db=None):
    file = open(ddl_file, "r+")
    filedata = file.readlines()

    filedata = "".join(filedata)
    filedata = filedata.lower()
    filedata = filedata.split("\ngo\n")

    tables = get_table_data(filedata)

    for table in tables:
        table_collection = "tables"
        table_document = table
        table_document["api_design_id"] = api_design_id
        table_document["ddl_file"] = ddl_filename

        # config.store_document(table_collection, table_document, db)

    return {"success": True, "status": 200, "message": "ok"}


parse_ddl_file("./../tmp/checkout_script.sql")
# parse_ddl_file("./../tmp/mdscript.sql")
