from pprint import pprint
import re
from api_designer.ddl_parser.ts import get_ts_order
from api_designer.ddl_parser import dtmapper



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
_DATA_TYPES_OTHER = ["sql_variant", "uniqueidentifier", "xml", "cursor", "table", "hierarchyid", "geography", "sysname"]

_DATA_TYPES_ALL = (
    _DATA_TYPES_NUMERIC + _DATA_TYPES_DATETIME + _DATA_TYPES_STRING + _DATA_TYPES_OTHER
)

class Parser:
    def __init__(self, filedata):
        self.filedata = filedata
        self.filedata = "".join(self.filedata)
        self.filedata = self.filedata.lower()
        self.filedata = self.filedata.split("\ngo\n")

        self.types = {}
        self.tables = []    # schema_name.table_name
        self.table_details = []
        self.constraint = re.compile("\[(\w+)\] +(asc|desc)")

    @staticmethod
    def preprocess_line(line, prefix):
        line = line.replace("\n", " ")
        line = re.sub("\t", " ", line)
        line = re.sub(" +", " ", line)
        line = line.strip()

        if prefix in line:
            start_index = line.find(prefix)
            line = line[start_index + len(prefix):]
            line = line.strip()
            return line
        return None

    # key - table or type name
    @staticmethod
    def extract_key_name(text, delim = " "):
        db_name = None
        schema_name = None
        key_name = None

        tmp = text.split(delim, 1)
        text = tmp[0]
        remain = tmp[1] if len(tmp) > 1 else None

        text = text.split(".")

        if len(text) == 1:
            key_name = re.sub("[\[\]]", "", text[0])
        elif len(text) == 2:
            schema_name = re.sub("[\[\]]", "", text[0])
            key_name = re.sub("[\[\]]", "", text[1])
        elif len(text) == 3:
            db_name = re.sub("[\[\]]", "", text[0])
            schema_name = re.sub("[\[\]]", "", text[1])
            key_name = re.sub("[\[\]]", "", text[2])

        return db_name, schema_name, key_name, remain

    def extract_type_data(self, line):
        line = line.split("from ", 1)
        type_data = line[1]
        type_data = re.sub("[\[\]]", "", type_data)
        type_data = type_data.split(" ", 1)
        base_type = type_data[0]
        type_data = type_data[1]

        if "not null" in type_data:
            constraint = "not null"
        elif "null" in type_data:
            constraint = "null"

        return base_type, constraint

    # prefix - create type
    def get_custom_types(self, lines):
        for line in lines:
            line = Parser.preprocess_line(line, "create type")
            if line:
                db_name, schema_name, key_name, remain = Parser.extract_key_name(line)
                base_type, constraint = self.extract_type_data(remain)

                if base_type.split("(")[0] in _DATA_TYPES_ALL:
                    if key_name not in self.types:
                        self.types[key_name] = {
                            "db": db_name,
                            "schema": schema_name,
                            "base_type": base_type,
                            "valueconstraint": constraint
                        }

    def extract_column_data(self, line):
        is_composite_key = False
        constraint_keys = []
        columns = []

        primary_key = None
        composite_key = []

        line = line.split("constraint ", 1)
        column_data = line[0]
        if len(line) == 2:
            constraint_data = line[1]
            constraint_keys = self.constraint.findall(constraint_data)
            constraint_keys = [x[0] for x in constraint_keys]
            is_composite_key = True if len(constraint_keys) > 1 else False

        column_data = re.split(', \[', column_data)
        column_data = list(filter(None, column_data))

        for cd in column_data:
            cd = re.sub("[\[\]]", "", cd)
            cd = cd.strip(" ,\n")
            cd = cd.split(" ")

            serial = False
            auto = False
            
            column_name = cd[0]
            tmp = cd[1]

            if tmp.split("(")[0] in _DATA_TYPES_ALL:
                column_type = tmp
            elif tmp.split(".")[-1] in self.types:
                column_type = tmp.split(".")[-1]
            else:
                column_type = None
                auto = True

            constraint = None
            if "not" in cd and "null" in cd:
                constraint = "not null"
            elif "null" in cd:
                constraint = "null"

            if "identity(1,1)" in cd:
                serial = True
                auto = True # Auto Insertion

            openapi_type = dtmapper.convert_sql_server_dtype(column_type)

            column = {
                'name': column_name,
                'type': column_type,
                'valueconstraint': constraint,
                'serial': serial,
                'auto': auto,
                'openapi': openapi_type
            }

            if column_name in constraint_keys:
                column['keyType'] = "composite" if is_composite_key else "primary"

            columns.append(column)
            if not is_composite_key and len(constraint_keys) == 1:
                primary_key = constraint_keys[0]
            else:
                composite_key = constraint_keys
            
        return columns, primary_key, composite_key

    # prefix - create table
    def get_table_data(self, lines):
        for line in lines:
            line = Parser.preprocess_line(line, "create table")
            if line:
                db_name, schema_name, table_name, remain = Parser.extract_key_name(line, delim="(")
                columns, primary_key, composite_keys = self.extract_column_data(remain)

                self.tables.append(f"{schema_name}.{table_name}")
                self.table_details.append({
                    "key": f"{schema_name}.{table_name}",
                    "schema": schema_name,
                    "table": table_name,
                    "primary": primary_key,
                    "composite": composite_keys,
                    "attributes": columns,
                    "conditions": []
                })

    def extract_alter_data(self, line):
        if "add constraint " in line:
            line = Parser.preprocess_line(line, "add constraint")
            
            # [key] default default_value for [column]
            if " default " in line:
                line = Parser.preprocess_line(line, " default ")
                line = line.split(" for ", 1)
                value = line[0]
                column = line[1]
                column = re.sub("[\[\]]", "", column)
                return {
                    "type": "default",
                    "column": column,
                    "value": value
                }

            elif "foreign key" in line and "references" in line:
                line = line.split(" references ", 1)
                column = line[0]
                reference = line[1]

                column = Parser.preprocess_line(column, "foreign key")
                column = column.strip("()[]")
                reference = re.sub("[\(\)]", "", reference)
                reference = re.sub("\] *\[", "][", reference)
                reference = re.sub("\]\[", "].[", reference)
                reference = Parser.extract_key_name(reference)

                if f"{reference[0]}.{reference[1]}" in self.tables:
                    return {
                        "type": "foreign",
                        "column": column,
                        "reference": {
                            "schema": reference[0],
                            "table": reference[1],
                            "column": reference[2],
                            "remain": reference[3]
                        }
                    }

            elif " check " in line:
                line = Parser.preprocess_line(line, "check ")
                return {
                    "type": "condition",
                    "value": line
                }

            return None

    # prefix - alter table
    def get_alter_conditions(self, lines):
        for line in lines:
            line = Parser.preprocess_line(line, "alter table")
            if line:
                db_name, schema_name, table_name, remain = Parser.extract_key_name(line)
                alter = self.extract_alter_data(remain)
                lookup_key = f"{schema_name}.{table_name}"

                if alter and "type" in alter:
                    if alter["type"] == "default":
                        for ti, td in enumerate(self.table_details):
                            if td['key'] == lookup_key:
                                for tc, tr in enumerate(td['attributes']):
                                    if tr['name'] == alter['column']:
                                        self.table_details[ti]['attributes'][tc]['default'] = alter['value']


                    elif alter["type"] == "foreign":
                        for ti, td in enumerate(self.table_details):
                            if td['key'] == lookup_key:
                                for tc, tr in enumerate(td['attributes']):
                                    if tr['name'] == alter['column']:
                                        self.table_details[ti]['attributes'][tc]['foreign'] = alter['reference']

                    elif alter["type"] == "condition":
                        for ti, td in enumerate(self.table_details):
                            if td['key'] == lookup_key:
                                self.table_details[ti]["conditions"].append(alter["value"])

    # topological sorting
    def get_table_insertion_order(self):
        tables = []

        for td in self.table_details:
            dependencies = set()
            for tc in td["attributes"]:
                if "foreign" in tc and "table" in tc["foreign"]:
                    tf = tc["foreign"]
                    tmp = f'{tf["schema"]}.{tf["table"]}'
                    dependencies.add(tmp)
            tables.append({
                "key": f'{td["schema"]}.{td["table"]}',
                "dependencies": list(dependencies)
            })
        
        table_order = get_ts_order(tables)
        return table_order

    def parse_data(self):
        try:
            self.get_custom_types(self.filedata)
            self.get_table_data(self.filedata)
            self.get_alter_conditions(self.filedata)
            self.get_table_insertion_order()

            #pprint(self.table_details)
            return self.table_details
        except Exception as e:
            return None