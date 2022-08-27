from api_designer import mongo
import random
from api_designer.dbgenerate.generator import Generator
import pandas as pd
import shortuuid

from api_designer.dbgenerate.ezsampler import Sampler
import copy

from pprint import pprint

FUNCTIONAL_COUNT = 3
PERFORMANCE_COUNT = 100


class DBGenerator:
    def __init__(self, projectid, generation_type, db):
        self.projectid = projectid
        self.generation_type = generation_type
        self.data = {}
        self.generate_count = PERFORMANCE_COUNT if generation_type == "performance" else FUNCTIONAL_COUNT
        self.db = db

    def fetch_table_data(self):
        self.db_document = self.db.database.find({"projectid": self.projectid})
        self.table_documents = self.db.tables.find({"projectid": self.projectid})

        self.db_document = list(self.db_document)[0]
        self.dbtype = self.db_document["type"]

        self.table_documents = list(self.table_documents)
        self.table_documents = {x["key"]: x for x in self.table_documents}

    def fetch_composite_keys(self, table_name):
        if "composite" in self.table_documents[table_name]:
            return self.table_documents[table_name]["composite"]
        else:
            return []


    def generate_mssql_query(self, table_key, columns, column_data, default_generated):
        query = f"INSERT INTO {table_key} ("
        for col in columns:
            query += f"[{col}], "
        query = query.strip(", ")
        query += " ) VALUES"

        tmp = ""
        for c in columns:
            cd = next(item for item in column_data if item["name"] == c)
            cd = cd.get("decoder", None)
            ctype = cd.get("type")
            cformat = cd.get("format")

            if ctype == "number":
                tmp += f"{{{c}}}, "
            elif ctype == "string":
                tmp += f"{{{c}}}, " if c in default_generated else f"'{{{c}}}', "
            elif ctype == "datetime":
                tmp += f"{{{c}}}, " if c in default_generated else f"'{{{c}}}', "
            else:
                if cformat == "xml":
                    tmp += f"'{{{c}}}', "
                elif cformat == "geometry":
                    tmp += f"geometry::STGeomFromText('{{{c}}}', 0), "
                elif cformat == "geography":
                    tmp += f"geography::STGeomFromText('{{{c}}}', 4326), "
                elif cformat == "binary":
                    tmp += f"{{{c}}}, "
                else:
                    tmp += f"{{{c}}}, " if c in default_generated else f"'{{{c}}}', "
        tmp = tmp.rstrip(", ")

        query = f"{query} ({tmp} )"
        return query

    def generate_postgres_query(self, table_key, columns, column_data, default_generated):
        query = f"INSERT INTO {table_key} ( {', '.join(columns)} ) VALUES "
        tmp = ""
        for c in columns:
            cd = next(item for item in column_data if item["name"] == c)
            cd = cd.get("decoder", None)
            ctype = cd.get("type")
            cformat = cd.get("format")

            if ctype == "number":
                tmp += f"{{{c}}}, "
            elif ctype == "string":
                tmp += f"{{{c}}}, " if c in default_generated else f"'{{{c}}}', "
            elif ctype == "datetime":
                tmp += f"{{{c}}}, " if c in default_generated else f"'{{{c}}}', "
            elif cformat == "array":
                tmp += f"'{{{c}}}', " 
            else:
                tmp += f"{{{c}}}, " if c in default_generated else f"'{{{c}}}', "

        tmp = tmp.rstrip(", ")
        query = f"{query} ({tmp} )"
        return query

    def generate_foreign_data(self, column, primary_keys, generate_pk = True):
        foreign = column["foreign"]
        key = f"{foreign['schema']}.{foreign['table']}"

        foreign_table = self.table_documents[key]
        foreign_column = next(item for item in foreign_table["attributes"] if item["name"] == foreign["column"])
        foreign_sample = foreign_column["sample"]
        foreign_column_name = foreign_column["name"]

        if column["name"] in primary_keys:
            if generate_pk:
                if self.dbtype == "mssql":
                    ret = f"(select top 1 {foreign_column_name} from {key} order by {foreign_column_name} desc)"
                elif self.dbtype == "postgres":
                    ret = f"(select {foreign_column_name} from {key} order by {foreign_column_name} limit 1)"
            else:
                ret = None
        else:
            foreign_examples = foreign_sample["samples"]
            foreign_examples += [None] * foreign_sample["null"]
            ret = random.choice(foreign_examples)

        return ret

    def generate_column_data(self, column, constraints, generated_object):
        ret = None
        sample = column.get("sample")
        decoder = column.get("decoder")

        column_constraints = None
        for cs in constraints:
            csp = cs.get("parsed")
            if csp and column["name"] in csp.get("columns"):
                column_constraints = csp["constraints"]

        if not sample or not decoder:
            return None
        else:
            G = Generator(column, column_constraints, generated_object)
            ret = G.generate_data()
        return ret

    @staticmethod
    def get_all_constraint_columns(constraints):
        ret = []
        for c in constraints:
            if "parsed" in c and c["parsed"]:
                tmp = c["parsed"].get("columns", [])
                if len(tmp) > 1:
                    ret += tmp
        return ret

    @staticmethod
    def get_all_primary_columns(data):
        ret = [data.get("primary")] if data.get("primary") else []
        ret += data.get("composite", [])
        return ret

    @staticmethod
    def get_composite_foreign(attributes):
        ret = {}
        for att in attributes:
            if "foreign" in att:
                foreign = att["foreign"]
                fk_key = foreign["key"]
                reference = f"{foreign['schema']}.{foreign['table']}.{foreign['column']}"
                column_name = att["name"]
                if fk_key not in ret:
                    ret[fk_key] = []
                ret[fk_key].append({"column": column_name, "reference": reference})

        ret = {x: y for x, y in ret.items() if len(y) > 1}
        return ret

    @staticmethod
    def get_table_df(data):
        if len(data) > 1:
            copy_data = copy.deepcopy(data)
            headers = copy_data.pop(0)
            df = pd.DataFrame(copy_data, columns=headers)
            df = df.astype(object).where(pd.notnull(df), None)
            return df
        return None

    def generate_testcase_data(self, table_name, selected_columns):
        table_details = self.table_documents[table_name]
        table_constraints = table_details["constraints"]
        table_attributes = table_details["attributes"]

        generated_data = {}

        if table_details.get("master"):
            table_df = DBGenerator.get_table_df(table_details["data"])
            generated_data = table_df.sample()[[selected_columns]]
            generated_data = generated_data.to_dict('records')[0]

        else:
            all_primary_columns = DBGenerator.get_all_primary_columns(table_details)
            all_constraint_columns = DBGenerator.get_all_constraint_columns(table_constraints)
            composite_foreigns = DBGenerator.get_composite_foreign(table_attributes)
            table_df = DBGenerator.get_table_df(table_details["data"])

            for ta in table_attributes:
                column_name = ta["name"]
                if column_name in selected_columns:
                    column_samples = table_df[column_name].tolist()
                    S = Sampler(column_samples)
                    ta["sample"] = S.get_sample_data()

                    if composite_foreigns:
                        for _, cf in composite_foreigns.items():
                            cf_table = cf[0]["reference"].rsplit(".", 1)[0]
                            cf_columns = [x["column"] for x in cf]
                            cf_references = [x["reference"].rsplit(".", 1)[1] for x in cf]
                            cf_data = self.table_documents[cf_table]
                            cf_data = DBGenerator.get_table_df(cf_data["data"])
                            cf_data = cf_data.sample()

                            for cfc, cfr in zip(cf_columns, cf_references):
                                generated_data[cfc] = cf_data[cfr].tolist()[0]

                    if column_name not in generated_data:
                        if ta.get("auto") or ta.get("default") or ta.get("foreign") or ta.get("computed"):
                            if ta.get("foreign"):
                                ret = self.generate_foreign_data(ta, all_primary_columns, generate_pk=False)
                                generated_data[column_name] = ret
                            elif column_name in all_constraint_columns:
                                ret = self.generate_column_data(ta, table_constraints, generated_data)
                            elif ta.get("default"):
                                if len(ta["default"]) >= 2 and ta["default"][0] == "'" and ta["default"][-1] == "'":
                                    ret = ta["default"].strip("'")
                                else:
                                    ret = ta["default"]
                                generated_data[column_name] = ret
                            elif ta.get("auto"):
                                generated_data[column_name] = "placeholder"
                        else:
                            ret = self.generate_column_data(ta, table_constraints, generated_data)
                            generated_data[column_name] = ret

        return generated_data