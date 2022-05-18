from pprint import pprint
import re
from api_designer.ddl_parser.ts import get_ts_order
from api_designer.ddl_parser.common_parser import remove_prefix
from api_designer.ddl_parser import dtmapper


# Reference - https://www.w3schools.com/sql/sql_datatypes.asp
_DATA_TYPES_NUMERIC_POSTGRES = [
    "smallint",
    "integer"
]
_DATA_TYPES_STRING_POSTGRES = [
    "character",
    "boolean",
    "tsvector",
    "bytea"
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
    "mediumtext",
    "mediumblob"
]
_DATA_TYPES_OTHER = ["sql_variant", "uniqueidentifier", "xml",
                     "cursor", "table", "hierarchyid", "geography", "sysname"]

_DATA_TYPES_ALL = (
    _DATA_TYPES_NUMERIC + _DATA_TYPES_DATETIME + _DATA_TYPES_STRING +
    _DATA_TYPES_OTHER + _DATA_TYPES_NUMERIC_POSTGRES + _DATA_TYPES_STRING_POSTGRES
)


class Parser:
    def __init__(self, filedata):
        self.filedata = filedata
        self.filedata = "".join(self.filedata)
        self.filedata = self.filedata.lower()
        self.filedata = self.filedata.split(
            "# ------------------------------------------------------------\n")

        self.types = {}
        self.tables = []    # schema_name.table_name
        self.table_details = []
        #self.constraint = re.compile("\[(\w+)\] +(asc|desc)")

    @staticmethod
    def preprocess_line(line, prefix):
        line = line.replace("\n\n\n", " ")
        line = re.sub("\t", " ", line)
        line = re.sub(" +", " ", line)
        line = line.strip()

        if prefix in line:
            start_index = line.find(prefix)
            line = line[start_index + len(prefix):]
            line = line.strip()
            return line
        return None

    def text_strip(self, text):
        return text.strip("`")

    def extract_table_data(self, text, db_type):
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

    def get_mysql_primary_and_composite_list(self, text):
        primary_key = "null"
        composite_key_list = []
        column_names_list = text.split('(')[1].split(',')
        if len(column_names_list) == 1:
            primary_key = column_names_list[0].strip("`")
        elif len(column_names_list) > 1:
            composite_key_list = list(map(self.text_strip, column_names_list))
        return primary_key, composite_key_list

    def extract_mysql_constraint_keys(self, text):
        text = text.strip(" \n")
        initial_list = []
        foreign_key_dict = {}
        tmp = text.split('(', 1)[1].split(')', 1)
        column_name = tmp[0]
        column_name = column_name.strip("`")
        reference_table_name = tmp[1].split("`", 1)[1].split("`", 1)[0]
        reference_table_column = tmp[1].split("`", 1)[1].split("`", 1)[
            1].split(')', 1)[0]
        reference_table_column = reference_table_column.strip(" `(")
        foreign_key_dict["schema"] = "None"
        foreign_key_dict["table"] = reference_table_name
        foreign_key_dict["column"] = reference_table_column
        initial_list.append(foreign_key_dict)
        return column_name, initial_list

    def extract_mysql_column_values(self, text):
        text = re.sub("[\[\]]", "", text)
        text = text.strip(" ,\n")
        # print(text)
        text = text.split(" ")
        # print("text after stripping:", text)

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
            column_values["default"] = "null"
            column_values["auto"] = False
            column_values["serial"] = False
            if "auto_increment" in text:
                column_values["auto"] = True
                column_values["serial"] = True

            openapi_type = dtmapper.convert_mysql_server_dtype(dt)
            column_values["openapi"] = openapi_type

            if len(text) >= 3 and "not" in text:
                column_values["valueconstraint"] = "not null"
            if "default" in text:
                if text[-2] == "default":
                    temp = text[-1].strip("`'")
                    column_values["default"] = temp
                elif text[-3] == "default":
                    temp = text[-2] + " " + text[-1].strip("`'")
                    column_values["default"] = temp

            return column_values
        return None

    def extract_mysql_column_data(self, text, table_name):
        primary_key = "null"
        composite_key_list = []
        foreignkey_dict = {}
        uniquekey_list = []
        column_values = []
        tmp = text.split(",")
        # print("tmp:",tmp)
        primaryKeyStatement = ""
        flag = -1
        for cc in tmp:

            if flag == 0:
                cc = cc.strip(" \n'")
                primaryKeyStatement = primaryKeyStatement + cc + ","
                if ")" in cc:
                    primaryKeyStatement = primaryKeyStatement.split(")", 1)[0]
                    flag = 1
                    primary_key, composite_key_list = self.get_mysql_primary_and_composite_list(
                        primaryKeyStatement)
            if cc.startswith("\n primary"):
                cc = cc.strip(" \n")
                flag = 0
                primaryKeyStatement = primaryKeyStatement + cc + ","
                if ")" in cc:
                    primaryKeyStatement = primaryKeyStatement.split(")", 1)[0]
                    flag = 1
                    primary_key, composite_key_list = self.get_mysql_primary_and_composite_list(
                        primaryKeyStatement)

            elif cc.startswith("\n constraint"):
                # print("foriegn keyyy")
                column_name, constraint_list = self.extract_mysql_constraint_keys(
                    cc)
                foreignkey_dict[column_name] = constraint_list
            elif cc.startswith("\n unique"):
                cc = cc.strip(" \n")
                column_name = cc.split('(')[1].split(')')[0]
                column_name = column_name.strip("`")
                uniquekey_list.append(column_name)
        counter = 0
        for cc in tmp:
            if cc.startswith("\n primary"):
                break
            if not cc.startswith(("\n primary", "\n key", "\n constraint", "\n unique")):
                if "decimal" in cc:
                    cc = cc + ", " + tmp[counter + 1][1:]
                    tmp.pop(counter + 1)
                ret = self.extract_mysql_column_values(cc)

                if ret and ret["name"] in uniquekey_list:
                    ret["keyType"] = "unique"
                elif ret and ret["name"] == primary_key:
                    ret["keyType"] = "primary"
                elif ret and ret["name"] in composite_key_list:
                    ret["keyType"] = "composite"
                elif ret and ret["name"] in foreignkey_dict.keys():
                    ret["foreign"] = foreignkey_dict[ret["name"]]
                elif ret:
                    ret["keyType"] = "null"
                # elif ret:
                #     ret["keys"] = {}
                column_values.append(ret)
            counter = counter + 1
        return primary_key, composite_key_list, column_values

    # key - table or type name
    def parse_data(self):
        try:
            lines = self.filedata
            res = []

            for line in lines:
                line = line.replace("\n\n\n", " ")
                line = re.sub("\t", " ", line)
                line = re.sub(" +", " ", line)
                line = line.strip()
                # print("line after removing spaces and \n and tabspaces:", line)
                if line.startswith("create table"):  # table found
                    if line.__contains__(";"):
                        tmp = line.split(";")[0].split("(", 1)
                        # print(tmp)
                    else:
                        tmp = line.split("(", 1)

                    if len(tmp) != 2:
                        continue

                    table_data = self.extract_table_data(tmp[0], "mysql")
                    key = table_data[1]
                    primary_key, composite_list, column_data = self.extract_mysql_column_data(
                        tmp[1], table_data[1])

                    dict1 = {
                        "schema": table_data[0],
                        "table": table_data[1],
                        "attributes": column_data,
                        "key": key,
                        "primary": primary_key,
                        "composite": composite_list
                    }

                    res.append(
                        dict1
                    )

                # if line.startswith("alter table"):
                #     get_alter_table_data(line)

            return res

            # pprint(self.table_details)
            # return self.table_details
        except Exception as e:
            return None
