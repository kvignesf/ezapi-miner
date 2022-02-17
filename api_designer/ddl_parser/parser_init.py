# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************



from api_designer import config
from api_designer.ddl_parser import dtmapper
from multipledispatch import dispatch
from pprint import pprint
import re

constraint = re.compile("\[(\w+)\] +(asc|desc)")

# Reference - https://www.w3schools.com/sql/sql_datatypes.asp

DATA_TYPES_NUMERIC_POSTGRES = [
    "smallint",
    "integer"
]
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
    "tinytext",
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

@dispatch(str)
def extract_table_data(text):
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

@dispatch(str, str)
def extract_table_data(text, db_type):
    schema_name = None
    table_name = None

    if db_type == "mysql":
        text = remove_prefix(text, "create table if not exists ")
        text = text.strip("`")
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
    text = text.strip(" ,\n")
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

        openapi_type = dtmapper.convert_sql_server_dtype(dt)
        column_values["openapi"] = openapi_type

        if "identity(1,1)" in text:
            column_values["serial"] = True

        if len(text) >= 3 and text[-1] == "not":
            column_values["valueconstraint"] = "not null"

        return column_values
    return None

def extract_mysql_column_values(text):
    text = re.sub("[\[\]]", "", text)
    text = text.strip(" ,\n")
    text = text.split(" ")
    print("text after stripping:", text)
    
    column_values = {}
    if len(text) >= 2:
        name, dt = text[0], text[1]
        name = name.strip("`")
        if dt.startswith("decimal("):
            dt = dt + " " + text[2]
        if dt.split("(")[0] not in _DATA_TYPES_ALL:
            print("*Error - Unidentified Data Type ", dt)
            return None

        column_values["name"] = name
        column_values["datatype"] = dt
        column_values["valueconstraint"] = "null"

        # openapi_type = dtmapper.convert_sql_server_dtype(dt)
        # column_values["openapi"] = openapi_type

        if len(text) >= 3 and "not" in text:
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

def extract_mysql_constraint_keys(text):
    text = text.strip(" \n")
    initial_list = []
    outer_dict = {}
    inner_dict = {}

    if text.startswith("primary"):
        column_name = text.split('(')[1].split(')')[0]
        column_name = column_name.strip("`")
        outer_dict["keyType"] = "primary"
        outer_dict["constraint"] = inner_dict
        print("important:",column_name,outer_dict)
        initial_list.append(outer_dict)
        return column_name, initial_list

    elif text.startswith("constraint"):
        tmp = text.split('(',1)[1].split(')',1)
        column_name = tmp[0]
        column_name = column_name.strip("`")
        reference_table_name = tmp[1].split("`",1)[1].split("`",1)[0]
        reference_table_column = tmp[1].split("`",1)[1].split("`",1)[1].split(')',1)[0]
        reference_table_column =reference_table_column.strip(" `(")
        outer_dict["keyType"] = "foreign"
        inner_dict["table"] = reference_table_name
        inner_dict["column"] = reference_table_column
        outer_dict["constraint"] = inner_dict
        initial_list.append(outer_dict)
        return column_name, initial_list


    elif text.startswith("unique"):
        column_name = text.split('(')[1].split(')')[0]
        column_name = column_name.strip("`")
        outer_dict["keyType"] = "unique"
        outer_dict["constraint"] = inner_dict
        print("important:",column_name,outer_dict)
        initial_list.append(outer_dict)
        return column_name, initial_list



def extract_mysql_column_data(text):
    primarykey_dict = {}
    foreignkey_dict = {}
    uniquekey_dict = {}
    column_values = []
    tmp = text.split(",")

    for cc in tmp:
        if cc.startswith("\n primary"):
            print("primary keyyy")
            column_name, constraint_list = extract_mysql_constraint_keys(cc)
            primarykey_dict[column_name] = constraint_list
        elif cc.startswith("\n constraint"):
            print("foriegn keyyy")
            column_name, constraint_list = extract_mysql_constraint_keys(cc)
            foreignkey_dict[column_name] = constraint_list
        elif cc.startswith("\n unique"):
            column_name, constraint_list = extract_mysql_constraint_keys(cc)
            uniquekey_dict[column_name] = constraint_list
    counter = 0
    for cc in tmp:
        
        if not cc.startswith(("\n primary", "\n key", "\n constraint", "\n unique")):
            if "decimal" in cc:
                cc = cc + ", " + tmp[counter+1][1:]
                tmp.pop(counter+1)
            ret = extract_mysql_column_values(cc)
            if ret and ret["name"] in uniquekey_dict.keys():
                ret["keys"] = uniquekey_dict[ret["name"]]
            elif ret and ret["name"] in primarykey_dict.keys():
                ret["keys"] = primarykey_dict[ret["name"]]
            elif ret and ret["name"] in foreignkey_dict.keys():
                ret["keys"] = foreignkey_dict[ret["name"]]
            elif ret:
                ret["keys"] = {}
            column_values.append(ret)
        counter = counter + 1
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
    print("lines", lines)
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


def parse_ddl_file(ddl_file, projectid=None, ddl_filename=None, db=None):
    file = open(ddl_file, "r+")

    filedata = file.readlines()
    print('filedata', filedata)
    filedata = "".join(filedata)
    filedata = filedata.lower()
    filedata = filedata.split("\ngo\n")
    print('filedata', filedata)

    tables = get_table_data(filedata)
    print('tables', tables)
    for table in tables:
        table_collection = "tables"
        table_document = table
        table_document["projectid"] = projectid
        table_document["ddl_file"] = ddl_filename

        config.store_document(table_collection, table_document, db)

    return {"success": True, "status": 200, "message": "ok"}


def get_db_table_data(lines, db_type):
    res = []
    for line in lines:
        line = line.replace("\n\n\n", " ")
        line = re.sub("\t", " ", line)
        line = re.sub(" +", " ", line)
        line = line.strip()
        print("line after removing spaces and \n and tabspaces:", line)
        if line.startswith("create table"):  # table found
            if line.__contains__(";"):
                tmp = line.split(";")[0].split("(", 1)
                print(tmp)
            else:
                print("!!!!!!!!!!")
                tmp = line.split("(", 1)

            if len(tmp) != 2:
                continue

            table_data = extract_table_data(tmp[0], db_type)
            if db_type == "mysql":
                column_data = extract_mysql_column_data(tmp[1])
            else:
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


def parse_db_ddl_file(ddl_file, projectid=None, ddl_filename=None, db_type=None, db=None):
    file = open(ddl_file, "r+")
    filedata = file.readlines()
    filedata = "".join(filedata)
    filedata = filedata.lower()

    if db_type == "postgres":
        filedata = filedata.split("--")
    elif db_type == "mysql":
        filedata = filedata.split("# ------------------------------------------------------------\n")
    else:
        filedata = filedata.split("\ngo\n")
    
    tables = get_db_table_data(filedata, db_type)
    print('tables', tables)
    for table in tables:
        table_collection = "tables"
        table_document = table
        table_document["projectid"] = projectid
        table_document["ddl_file"] = ddl_filename

        config.store_document(table_collection, table_document, db)

    return {"success": True, "status": 200, "message": "ok"}
