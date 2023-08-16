import psycopg2
import re
from sqlalchemy import create_engine, text

from api_designer.sql_connect.ts2 import get_ts_order
from api_designer.sql_connect.postgres_decoder import DTDecoder
from api_designer.sql_connect.postgres_openapi import DTMapper
from api_designer.sql_connect.ezsampler import Sampler
from api_designer.sql_connect.utils import *

_SYSTEM_SCHEMAS = ['pg_toast', 'pg_catalog', 'information_schema']
regex = re.compile('[@_!#$%^&*()<>?/\|}{~:]')

class Extractor:
    def __init__(self, dbtype, args):
        self.dbtype = dbtype
        self.engine = create_engine('postgresql+psycopg2://', connect_args = args)
        self.conn = self.engine.connect()

        self.schemas = []
        self.tables = []
        self.table_size = {}
        self.table_keys = {}
        self.foreign = {}
        self.user_defined_types = {}
        self.table_details = {}
        self.table_constraints = {}
        self.computed_columns = {}
        self.insertion_order = None
        self.table_data = {}
        self.sample_data = {}
        self.master_tables = []

    def get_schemas(self):
        self.schemas = self.conn.execute(text("select schema_name from information_schema.schemata"))
        self.schemas = [x[0] for x in self.schemas if x[0] not in _SYSTEM_SCHEMAS]

    def get_tables(self):
        for s in self.schemas:
            res = self.conn.execute(text(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{s}' and table_type = 'BASE TABLE'"))
            res = [f"{s}.{x[0]}" for x in res]
            self.tables += res

    def get_table_size(self):

        for s in self.tables:
            q = s
            if (regex.search(s) == None):
                q = s
            else:
                q = s.split(".")[0] + "." + "\"" + s.split(".")[1] + "\""
                #print ("q..", q)
            res = self.conn.execute(text(f"SELECT COUNT(*) FROM {q}"))
            #print( res )
            res = next(res)
            self.table_size[s] = res[0]

    # Reference - https://dataedo.com/kb/query/postgresql/list-of-foreign-keys-with-columns
    def get_foreign_relations(self):
        query = """
            select kcu.table_schema || '.' || kcu.table_name as foreign_table,
                rel_kcu.table_schema || '.' || rel_kcu.table_name as primary_table,
                kcu.ordinal_position as no,
                kcu.column_name as fk_column,
                rel_kcu.column_name as pk_column,
                kcu.constraint_name
            from information_schema.table_constraints tco
            join information_schema.key_column_usage kcu
                on tco.constraint_schema = kcu.constraint_schema
                and tco.constraint_name = kcu.constraint_name
            join information_schema.referential_constraints rco
                on tco.constraint_schema = rco.constraint_schema
                and tco.constraint_name = rco.constraint_name
            join information_schema.key_column_usage rel_kcu
                on rco.unique_constraint_schema = rel_kcu.constraint_schema
                and rco.unique_constraint_name = rel_kcu.constraint_name
                and kcu.ordinal_position = rel_kcu.ordinal_position
            where tco.constraint_type = 'FOREIGN KEY'
            order by kcu.table_schema,
                kcu.table_name,
                kcu.ordinal_position
        """
        res = self.conn.execute(text(query))
        columns = list(res.keys())
        res = [x for x in res]

        for r in res:
            tmp = {x:y for x,y in zip(columns, r)}
            key = f"{tmp['foreign_table']}.{tmp['fk_column']}"
            value = f"{tmp['primary_table']}.{tmp['pk_column']}"
            self.foreign[key] = {
                'key': tmp['constraint_name'],
                'schema': tmp['primary_table'].split('.', 1)[0],
                'table': tmp['primary_table'].split('.', 1)[1],
                'column': tmp['pk_column']
            }

    # Reference - https://dataedo.com/kb/query/postgresql/list-all-primary-keys-and-their-columns
    def get_table_keys(self):
        for s in self.schemas:
            query = f"""
                select kcu.table_schema,
                    kcu.table_name,
                    tco.constraint_name,
                    kcu.ordinal_position as position,
                    kcu.column_name as key_column
                from information_schema.table_constraints tco
                join information_schema.key_column_usage kcu 
                    on kcu.constraint_name = tco.constraint_name
                    and kcu.constraint_schema = tco.constraint_schema
                    and kcu.constraint_name = tco.constraint_name
                where tco.constraint_type = 'PRIMARY KEY' and kcu.table_schema = '{s}'
                order by kcu.table_schema,
                    kcu.table_name,
                    position;
            """
            res = self.conn.execute(text(query))
            columns = list(res.keys())
            res = [x for x in res]

            for r in res:
                tmp = {x:y for x,y in zip(columns, r)}
                key = f"{s}.{tmp['table_name']}"
                if key not in self.table_keys:
                    self.table_keys[key] = []
                self.table_keys[key].append(tmp['key_column'])

    @staticmethod
    def decode_table_attributes(data):
        data = {
            'datatype': str(data['data_type']),
            'valueconstraint': 'null' if data['is_nullable'] == 'YES' else 'not null',
            'default': data['column_default']
        }
        data['auto'] = True if data['default'] else False
        data['serial'] = True if (data['default'] and 'nextval(' in data['default']) else False
        return data

    def get_master_tables(self):
        #print("table_details", self.table_details)
        for tk, tv in self.table_details.items():
            table_size = self.table_size[tk]
            pk_columns = 0
            table_columns = 0
            table_foreign = 0
            column_names = []
            for col, col_data in tv.items():
                foreign_key = f"{tk}.{col}"
                if col_data["auto"]:
                    continue
                elif tk in self.table_keys and col in self.table_keys[tk]:
                    pk_columns += 1
                elif foreign_key in self.foreign:
                    table_foreign += 1
                else:
                    table_columns += 1
                    column_names.append(col)

            if (table_foreign + table_columns + pk_columns) <= 3 and table_foreign <= 1 and (table_columns + pk_columns)> 0 and table_size <= 250:
                master = True
                for col in column_names:
                    if len(self.sample_data[tk]) > 0:
                        col_sample = self.sample_data[tk][col]
                        col_details = self.table_details[tk][col]['decoder']
                        if (col_sample.get('repeat') != 1) or (col_details.get("type") not in ("number", "string")):
                            master = False

                if master:
                    self.master_tables.append(tk)

    def get_table_details(self):
        for t in self.tables:
            t_schema, t_name = t.split(".")
            column_desc = self.conn.execute(text(f"SELECT * from information_schema.columns where table_schema='{t_schema}' and table_name = '{t_name}'"))
            column_keys = column_desc.keys()
            column_desc = list(column_desc)
            
            attributes = {}
            generated = {}

            for cd in column_desc:
                tmp = {x:y for x, y in zip(column_keys, cd)}
                attributes[tmp['column_name']] = Extractor.decode_table_attributes(tmp)

                D = DTDecoder(tmp)
                attributes[tmp['column_name']]['decoder'] = D.decoder()

                D = DTMapper(tmp, self.user_defined_types)
                attributes[tmp['column_name']]['openapi'] = D.decoder()

                if "is_generated" in tmp and "generation_expression" in tmp and tmp["is_generated"] == "ALWAYS":
                    generated[tmp['column_name']] = tmp['generation_expression']

            self.table_details[t] = attributes
            self.computed_columns[t] = generated

    @staticmethod
    def parse_constraint(text, fields):
        ret = None
        try:
            if text and len(text) >= 4 and text[:2] == '((' and text[-2:] == '))':
                text = text[2:-2]

            columns = []
            for f in fields:
                if f in text:
                    columns.append(f)

            join_type = None
            if " OR " in text and " AND " in text:
                return ret
            elif " OR " in text:
                join_type = "or"
            elif " AND " in text:
                join_type = "and"

            constraints = re.split("OR|AND", text)
            constraints = [x.strip() for x in constraints]

            valid_constraints = []
            for c in constraints:
                if contains_in_list(c, columns):
                    valid_constraints.append(c)

            parsed_constraints = []
            for v in valid_constraints:
                sep = [">=", "<=", "=", "<>", ">", "<", "!=", " IS NOT ", " IS "]
                matched = None
                for s in sep:
                    if s in v:
                        matched = s
                        v = v.split(s, 1)
                        break

                if matched == '<>':
                    matched = '!='
                elif matched == ' IS NOT ':
                    matched = '!='
                elif matched == ' IS ':
                    matched = '=='

                if matched:
                    lhs = v[0].strip()
                    rhs = v[1].strip()

                    lhs = lhs.strip("(").strip(")")
                    rhs = rhs.strip("(").strip(")")

                    lfound, rfound = False, False

                    if lhs in columns:
                        lfound = True

                    if rhs in columns:
                        rfound = True

                    if lfound and rfound:
                        parsed_constraints.append({
                            "lhs": lhs,
                            "rhs": rhs,
                            "condition": matched,
                            "type": "both"
                        })
                    elif lfound and not rfound:
                        if rhs == 'NULL':
                            rhs = 'None'
                        try:
                            rhs = eval(rhs)
                            parsed_constraints.append({
                                "lhs": lhs,
                                "rhs": rhs,
                                "condition": matched,
                                "type": "lhs"
                            })
                        except:
                            pass
                    elif rfound and not lfound:
                        if lhs == 'NULL':
                            lhs = 'None'
                        try:
                            lhs = eval(lhs)
                            parsed_constraints.append({
                                "lhs": lhs,
                                "rhs": rhs,
                                "condition": matched,
                                "type": "rhs"
                            })
                        except:
                            pass

            ret = {
                "join_type": join_type,
                "columns": columns,
                "constraints": parsed_constraints
            }
        except Exception as e:
            pass
        return ret

    def get_check_constraints(self):
        query = """
        select tc.table_schema as table_schema,
            tc.table_name as table_name,
            string_agg(col.column_name, ', ') as column_name,
            tc.constraint_name as constraint_name,
            cc.check_clause as definition
        from information_schema.table_constraints tc
        join information_schema.check_constraints cc
            on tc.constraint_schema = cc.constraint_schema
            and tc.constraint_name = cc.constraint_name
        join pg_namespace nsp on nsp.nspname = cc.constraint_schema
        join pg_constraint pgc on pgc.conname = cc.constraint_name
                            and pgc.connamespace = nsp.oid
                            and pgc.contype = 'c'
        join information_schema.columns col
            on col.table_schema = tc.table_schema
            and col.table_name = tc.table_name
            and col.ordinal_position = ANY(pgc.conkey)
        where tc.constraint_schema not in('pg_catalog', 'information_schema')
        group by tc.table_schema,
                tc.table_name,
                tc.constraint_name,
                cc.check_clause
        order by tc.table_schema,
                tc.table_name
        """
        res = self.conn.execute(text(query))
        column_keys = list(res.keys())
        res = [x for x in res]

        for r in res:
            tmp = {x:y for x,y in zip(column_keys, r)}
            key = tmp['table_schema'] + '.' + tmp['table_name']
            if key not in self.table_constraints:
                self.table_constraints[key] = []
            fields = tmp['column_name'].split(",")
            fields = [x.strip() for x in fields]
            self.table_constraints[key].append({
                'name': tmp['constraint_name'],
                'column': fields,
                'definition': tmp['definition'],
                'parsed': Extractor.parse_constraint(tmp['definition'], fields)
            })
            
    @staticmethod
    def is_definite_column(data):
        data = list(filter(None, data))
        nlen = len(data)
        if not data:
            return False, None

        if isinstance(data[0], str):
            maxlen = max([len(x) for x in data])
            column_set = list(set(data))
            if maxlen <= 100 and len(column_set) * 3 <= nlen:
                return True, column_set

        elif isinstance(data[0], int):
            maxval = max([abs(x) for x in data])
            column_set = list(set(data))
            if maxval <= 10 and len(column_set) * 3 <= nlen:
                return True, column_set

        return False, None

    def get_sample_data(self):
        for t in self.tables:
            q = t
            if (regex.search(t) == None):
                q = t
            else:
                q = t.split(".")[0] + "." + "\"" + t.split(".")[1] + "\""
                #print ("q..", q)
            table_data = self.conn.execute(text(f"select * from {q} order by random() limit 100"))
            table_keys = list(table_data.keys())
            table_data = [list(x) for x in table_data]
            table_data_with_header = [table_keys] + table_data
            self.table_data[t] = table_data_with_header
            
            table_sample_data = {}
            if table_data and len(table_data) > 0:
                transposed_data = list(zip(*table_data))
                for idx, col in enumerate(table_keys):
                    column_data = transposed_data[idx]
                    S = Sampler(None, column_data)
                    table_sample_data[col] = S.get_sample_data()
                self.sample_data[t] = table_sample_data
            else:
                self.sample_data[t] = {}

    def get_insertion_order(self):
        tables = []
        #print("tbls", self.tables)
        #print("frgn", self.foreign.items())
        for t in self.tables:
            foreign_dependencies = set()

            for ft, fr in self.foreign.items():
                if t == ft.rsplit(".", 1)[0]:
                    tmp = f"{fr['schema']}.{fr['table']}"
                    foreign_dependencies.add(tmp)
            tables.append({
                "key": t,
                "dependencies": list(foreign_dependencies)
            })
            #print("tables1", tables)

        self.insertion_order = get_ts_order(tables)

    def prepare_db_document(self):
        document = {
            'projectid': self.projectid,
            'type': self.dbtype,
            'schemas': self.schemas,
            'tables': self.tables,
            'order': self.insertion_order
        }
        return document

    def prepare_table_document(self):
        table_documents = []
        for t in self.tables:
            keys = self.table_keys.get(t, [])
            keyType = None
            if len(keys) == 1:
                keyType = 'primary'
            elif len(keys) > 1:
                keyType = 'composite'

            document = {
                'projectid': self.projectid,
                'key': t,
                'schema': t.split('.')[0],
                'table': t.split('.')[1],
                'primary': keys[0] if keyType == 'primary' else None,
                'composite': keys if keyType == 'composite' else [],
                'master': True if t in self.master_tables else False,
                'attributes': [],
                'constraints': self.table_constraints[t] if t in self.table_constraints else [],
                'data': self.table_data[t]
            }

            for col, col_data in self.table_details[t].items():
                col_data['name'] = col
                col_data['keyType'] = keyType if col in keys else None
                
                foreign_key = f"{t}.{col}"
                if foreign_key in self.foreign:
                    col_data['foreign'] = self.foreign[foreign_key]

                col_data["sample"] = self.sample_data[t].get(col)

                if t in self.computed_columns and col in self.computed_columns[t]:
                    col_data['computed'] = self.computed_columns[t][col]

                document['attributes'].append(col_data)
            table_documents.append(document)
        return table_documents

    def extract_data(self, projectid):
        self.projectid = projectid
        self.get_schemas()
        self.get_tables()
        self.get_table_size()
        self.get_foreign_relations()
        self.get_table_keys()
        self.get_table_details()
        self.get_check_constraints()
        self.get_sample_data()
        self.get_master_tables()
        self.get_insertion_order()
        db_document = self.prepare_db_document()
        table_documents = self.prepare_table_document()

        # import json
        # json_data = json.dumps(table_documents, indent=4, sort_keys=True, default=str)
        # with open('dvdrental.json', 'w') as outfile:
        #     outfile.write(json_data)

        return db_document, table_documents