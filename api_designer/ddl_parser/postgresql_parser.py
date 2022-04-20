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
        self.filedata = self.filedata.split("--")

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

    # @dispatch(str)
    def extract_table_data(self, text):
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

    def postgres_constraints_dict(self, lines):
        table_constraints = {}
        manual_datatype_list = []

        for nline in lines:
            newlines = nline.split(";")
            for line in newlines:
                table_dict = {}
                new_list = []
                # line = re.sub("\t", " ", line)
                line = line.strip()
                if line.startswith("alter table") and line.__contains__("primary key"):
                    tmp = line.split("\n")
                    table_name = tmp[0].split(".")[1]
                    #print("alter table", table_name)
                    if "primary key" in tmp[1]:
                        tmp[1] = tmp[1].split("(")[1].split(")")[0].split(",")
                        if len(tmp[1]) > 1:
                            for each_item in tmp[1]:
                                each_item = each_item.strip(" ")
                                new_list.append(each_item)

                            table_dict["composite"] = new_list
                            table_dict["primary"] = "None"
                            table_constraints[table_name] = table_dict
                            # print("KeyType: composite")
                        else:
                            table_dict["composite"] = []
                            table_dict["primary"] = tmp[1][0]
                            table_constraints[table_name] = table_dict
                    else:
                        foreign_dict = {}
                        empty_dict = {}
                        foriegn_dict_keys = {}
                        tmpp = tmp[1].split("references")
                        column_name = tmpp[0].split("(", 1)[1].split(")", 1)[0]
                        tmpp[1] = tmpp[1].strip(" ")
                        temp = tmpp[1].split(".", 1)
                        reference_schema = temp[0]
                        tempp = temp[1].split("(", 1)
                        reference_table = tempp[0]
                        reference_column = tempp[1].split(")", 1)[0]
                        foreign_dict["column"] = reference_column
                        foreign_dict["schema"] = reference_schema
                        foreign_dict["table"] = reference_table
                        if "foreign_keys" in table_constraints[table_name].keys():
                            table_constraints[table_name]["foreign_keys"][column_name] = foreign_dict
                        else:
                            foriegn_dict_keys[column_name] = foreign_dict
                            table_constraints[table_name]["foreign_keys"] = foriegn_dict_keys
                            # table_constraints[table_name] = table_dict
                elif line.startswith("create type") or line.startswith("create domain"):
                    tmp = line.split("\n")
                    tmpp = tmp[0].split(" ")
                    manual_datatype_list.append(tmpp[2])
                elif line.startswith("alter table"):
                    table_dict["composite"] = []
                    table_dict["primary"] = []
                    tmp = line.split("\n")
                    table_name = (tmp[0].split(".")[1]).split(" ")[0]
                    if (table_name):
                        table_constraints[table_name] = table_dict

        return table_constraints, manual_datatype_list

    def extract_postgres_column_values(self, text, table_constraints_dict, dataype_list):
        flag = 0
        text = re.sub("[\[\]]", "", text)
        text = text.strip(" ,\n")
        temp = text
        text = text.split(" ")
        column_values = {}

        if len(text) >= 2:
            name, dt = text[0], text[1]
            if not "numeric" in dt:
                dt = dt.strip("\n")
                if "\n" in dt:
                    if dt[-2:] == "\n)":
                        dt = dt.strip(")")
                    dt = dt.replace("\n", "")
                    #if "))" in dt:
                    #    dt = dt.replace("))", ")")
                    #if ")" in dt and "(" not in dt:
                    #    dt = dt.replace(")", "")

            column_values["name"] = name
            if dt.split("(")[0] not in _DATA_TYPES_ALL:
                if dt in dataype_list:
                    column_values["datatype"] = "user_defined"
                    flag = 1
                else:
                    print("*Error - Unidentified Data Type ", dt)
                    return None

            if "character" == dt:
                dt = dt + " " + text[2]
            if "timestamp" == dt:
                dt = dt + " " + text[2] + " " + text[3] + " " + text[4]
            #print("dt:", dt)
            if flag == 0:
                column_values["datatype"] = dt
            column_values["valueconstraint"] = "null"
            column_values["default"] = "null"
            column_values["auto"] = False
            column_values["serial"] = False

            if "foreign_keys" in table_constraints_dict.keys() and name in table_constraints_dict[
                    "foreign_keys"].keys():
                column_values["foreign"] = table_constraints_dict["foreign_keys"][name]
            if name == table_constraints_dict['primary']:
                column_values["KeyType"] = 'primary'
            elif name in table_constraints_dict['composite']:
                column_values["KeyType"] = 'composite'
            else:
                column_values["KeyType"] = 'null'

            openapi_type = dtmapper.convert_postgres_server_dtype(dt)
            column_values["openapi"] = openapi_type

            if "default" in text:
                column_values["auto"] = True
                if "nextval" in temp:
                    column_values["serial"] = True

            if len(text) >= 3 and text[-2] == "not":
                column_values["valueconstraint"] = "not null"
                if "default" in text:
                    column_values["default"] = text[-3]
            elif "default" in text:
                column_values["default"] = text[-1]

            return column_values
        return None

    def extract_postgres_column_data(self, text, table_constraints_dict, datatype_list):
        column_values = []
        # print("postgresText:",text)
        tmp = text.split(",")
        # print("tmp:",tmp)
        counter = 0
        for cc in tmp:
            cc = cc.strip(" \n")
            if "numeric" in cc:
                #print("unformatted:", cc)
                cc = cc + "," + tmp[counter + 1]
                #print("formatted:", cc)
                tmp.pop(counter + 1)
            ret = self.extract_postgres_column_values(
                cc, table_constraints_dict, datatype_list)
            column_values.append(ret)
            counter = counter + 1
        return column_values

    # key - table or type name
    def parse_data(self):
        try:
            lines = self.filedata
            res = []
            table_constraints, manual_datatype_list = self.postgres_constraints_dict(
                lines)

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

                    table_data = self.extract_table_data(tmp[0])
                    key = table_data[0] + "." + table_data[1]
                    #print("table", key)
                    primary_key = table_constraints[table_data[1]]["primary"]
                    composite_list = table_constraints[table_data[1]
                                                       ]["composite"]
                    column_data = self.extract_postgres_column_data(tmp[1], table_constraints[table_data[1]],
                                                                    manual_datatype_list)

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
            print("ex", e)
            return None
