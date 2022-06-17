import re
from sqlalchemy import create_engine

from api_designer.sql_connect.ts2 import get_ts_order
from api_designer.sql_connect.mssql_decoder import DTDecoder
from api_designer.sql_connect.mssql_openapi import DTMapper
from api_designer.sql_connect.ezsampler import Sampler
from api_designer.sql_connect.utils import *

TO_STRING_DTYPES = ['geography', 'hierarchyid', 'geometry']

class Extractor:
    def __init__(self, dbtype, url):
        self.dbtype = dbtype
        self.engine = create_engine(url)
        self.conn = self.engine.connect()

        self.schemas = []
        self.tables = []
        self.table_size = {}
        self.user_defined_types = {}
        self.table_keys = {}
        self.foreign = {}
        self.table_details = {}
        self.table_constraints = {}
        self.computed_columns = {}
        self.insertion_order = None
        self.table_data = {}
        self.sample_data = {}
        self.master_tables = []

    def get_schemas(self):
        self.schemas = self.conn.execute("""
            select s.name
            from sys.schemas s
            inner join sys.sysusers u
            on u.uid = s.principal_id
            where u.name = 'dbo'
        """)
        self.schemas = [x[0] for x in self.schemas]

    def get_tables(self):
        for s in self.schemas:
            res = self.conn.execute(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = '{s}' and TABLE_TYPE = 'BASE TABLE'")
            res = [f"{s}.{x[0]}" for x in res]
            self.tables += res

    def get_table_size(self):
        for s in self.tables:
            res = self.conn.execute(f"SELECT COUNT(*) FROM {s}")
            res = next(res)
            self.table_size[s] = res[0]

    def get_user_defined_types(self):
        query = """
            SELECT t1.name as name, 
                t2.name as basetype, 
                t1.precision, 
                t1.scale, 
                t1.max_length as length, 
                t1.is_nullable 
            FROM sys.types t1 
            JOIN sys.types t2 
            ON t2.system_type_id = t1.system_type_id and t2.is_user_defined = 0 
            WHERE t1.is_user_defined = 1 and t2.name <> 'sysname' order by t1.name;
        """
        res = self.conn.execute(query)
        columns = list(res.keys())
        res = [x for x in res]

        for r in res:
            tmp = {x:y for x, y in zip(columns, r)}
            self.user_defined_types[r['name']] = tmp

    # Reference - https://stackoverflow.com/a/18929992
    def get_foreign_relations(self):
        query = """
            SELECT  obj.name AS FK_NAME,
                sch.name AS schema_name,
                tab1.name AS foreign_table,
                col1.name AS fk_column,
                tab2.name AS primary_table,
                sch2.name AS primary_schema,
                col2.name AS pk_column
            FROM sys.foreign_key_columns fkc
            INNER JOIN sys.objects obj
                ON obj.object_id = fkc.constraint_object_id
            INNER JOIN sys.tables tab1
                ON tab1.object_id = fkc.parent_object_id
            INNER JOIN sys.schemas sch
                ON tab1.schema_id = sch.schema_id
            INNER JOIN sys.columns col1
                ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
            INNER JOIN sys.tables tab2
                ON tab2.object_id = fkc.referenced_object_id
            INNER JOIN sys.schemas sch2
                ON tab2.schema_id = sch2.schema_id
            INNER JOIN sys.columns col2
                ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id;
        """
        res = self.conn.execute(query)
        columns = list(res.keys())
        res = [x for x in res]

        for r in res:
            tmp = {x:y for x,y in zip(columns, r)}
            key = f"{tmp['schema_name']}.{tmp['foreign_table']}.{tmp['fk_column']}"
            value = f"{tmp['primary_schema']}.{tmp['primary_table']}.{tmp['pk_column']}"
            self.foreign[key] = {
                'key': tmp['FK_NAME'],
                'schema': tmp['primary_schema'],
                'table': tmp['primary_table'],
                'column': tmp['pk_column']
            }

    # Reference - https://dataedo.com/kb/query/sql-server/list-all-primary-keys-in-database
    def get_table_keys(self):
        query = """
            select schema_name(tab.schema_id) as [schema_name], 
                pk.[name] as pk_name,
                substring(column_names, 1, len(column_names)-1) as [columns],
                tab.[name] as table_name
            from sys.tables tab
                inner join sys.indexes pk
                    on tab.object_id = pk.object_id 
                    and pk.is_primary_key = 1
            cross apply (select col.[name] + ', '
                                from sys.index_columns ic
                                    inner join sys.columns col
                                        on ic.object_id = col.object_id
                                        and ic.column_id = col.column_id
                                where ic.object_id = tab.object_id
                                    and ic.index_id = pk.index_id
                                        order by col.column_id
                                        for xml path ('') ) D (column_names)
            order by schema_name(tab.schema_id), pk.[name]
        """
        res = self.conn.execute(query)
        column_keys = list(res.keys())
        res = [x for x in res]

        for r in res:
            tmp = {x:y for x,y in zip(column_keys, r)}
            key = f"{tmp['schema_name']}.{tmp['table_name']}"
            if key not in self.table_keys:
                self.table_keys[key] = []
            columns = tmp['columns'].split(',')
            self.table_keys[key] += columns

    @staticmethod
    def decode_table_attributes(data):
        data = {
            'datatype': str(data['TYPE_NAME']),
            'valueconstraint': 'null' if data['IS_NULLABLE'] == 'YES' else 'not null',
            'default': data['COLUMN_DEF']
        }
        data['auto'] = True if (data['default'] or (' identity' in data['datatype'])) else False
        data['serial'] = True if (data['default'] and 'newid(' in data['default']) or (' identity' in data['datatype']) else False
        return data

    def get_master_tables(self):
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
                # elif "decoder" in col_data and col_data["decoder"].get("type") == "datetime":
                #     continue
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
                    col_sample = self.sample_data[tk][col]
                    col_details = self.table_details[tk][col]['decoder']
                    if (col_sample.get('repeat') != 1) or (col_details.get("type") not in ("number", "string")):
                        master = False

                if master:
                    self.master_tables.append(tk)

    def get_table_details(self):
        for t in self.tables:
            t_schema, t_name = t.split(".")
            column_desc = self.conn.execute(f"exec sp_columns @table_name=N'{t_name}', @table_owner=N'{t_schema}'")
            column_keys = column_desc.keys()
            column_desc = list(column_desc)

            attributes = {}
            for cd in column_desc:
                tmp = {x:y for x, y in zip(column_keys, cd)}
                # attributes[tmp['COLUMN_NAME']] = {}
                attributes[tmp['COLUMN_NAME']] = Extractor.decode_table_attributes(tmp)

                D = DTDecoder(tmp, self.user_defined_types)
                attributes[tmp['COLUMN_NAME']]['decoder'] = D.decoder()
                
                D = DTMapper(tmp, self.user_defined_types)
                attributes[tmp['COLUMN_NAME']]['openapi'] = D.decoder()
            self.table_details[t] = attributes

    @staticmethod
    def parse_constraint(text):
        ret = None
        try:
            if text and text[0] == '(' and text[-1] == ')':
                text = text[1:-1]

            columns = re.findall(r'\[.*?\]', text)
            columns = [re.sub('[\[\]]', '', x) for x in columns]
            columns = list(set(columns))

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
                sep = [">=", "<=", "=", "<>", ">", "<", "!=", " IS ", " IS NOT "]
                matched = None
                for s in sep:
                    if s in v:
                        matched = s
                        v = v.split(s, 1)
                        break

                if matched == '<>':
                    matched = '!='
                elif matched == ' IS ':
                    matched = '=='
                elif matched == ' IS NOT ':
                    matched = '!='

                if matched:
                    lhs = v[0]
                    rhs = v[1]
                    lfound, rfound = False, False

                    if contains_in_list(lhs, columns):
                        lhs = re.findall(r'\[.*?\]', lhs)
                        lhs = [re.sub('[\[\]]', '', x) for x in lhs]
                        lhs = lhs[0]
                        lfound = True

                    if contains_in_list(rhs, columns):
                        rhs = re.findall(r'\[.*?\]', rhs)
                        rhs = [re.sub('[\[\]]', '', x) for x in rhs]
                        rhs = rhs[0]
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
        select con.[name] as constraint_name,
            schema_name(t.schema_id) + '.' + t.[name]  as table_key,
            col.[name] as column_name,
            con.[definition],
            case when con.is_disabled = 0
                then 'Active'
                else 'Disabled'
                end as [status]
        from sys.check_constraints con
            left outer join sys.objects t
                on con.parent_object_id = t.object_id
            left outer join sys.all_columns col
                on con.parent_column_id = col.column_id
                and con.parent_object_id = col.object_id
        order by con.name
        """
        res = self.conn.execute(query)
        column_keys = list(res.keys())
        res = [x for x in res]

        for r in res:
            tmp = {x:y for x,y in zip(column_keys, r)}
            key = tmp['table_key']
            if key not in self.table_constraints:
                self.table_constraints[key] = []
            self.table_constraints[key].append({
                'name': tmp['constraint_name'],
                'column': tmp['column_name'],
                'definition': tmp['definition'],
                'status': tmp['status'],
                'parsed': Extractor.parse_constraint(tmp['definition'])
            })

    def get_computed_columns(self):
        query = """
        select ss.name as schema_name,
            tt.name as table_name,
            cc.name as column_name,
            cc.definition as definition
        from sys.computed_columns cc
            INNER JOIN sys.tables tt ON cc.object_id = tt.object_id
            INNER JOIN sys.schemas ss ON ss.schema_id = tt.schema_id;
        """
        res = self.conn.execute(query)
        column_keys = list(res.keys())
        res = [x for x in res]

        for r in res:
            tmp = {x:y for x,y in zip(column_keys, r)}
            key = f"{tmp['schema_name']}.{tmp['table_name']}"
            if key not in self.computed_columns:
                self.computed_columns[key] = {}
            self.computed_columns[key][tmp['column_name']] = tmp['definition']

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

    # Reference - https://github.com/mkleehammer/pyodbc/wiki/Using-an-Output-Converter-function
    # Reference - https://stackoverflow.com/a/70511716
    def get_sample_data(self):
        # connection = self.conn.connection
        # connection.add_output_converter(-151, str)
        for t in self.tables:
            columns = self.table_details[t]
            columns = {x:y['datatype'].split(" ")[0].lower() for x, y in columns.items()}

            query = ''
            for k, v in columns.items():
                if v not in TO_STRING_DTYPES:
                    query += f' "{k}",'
                else:
                    query += f' "{k}".ToString() as {k},'
            query = query.strip(",")
            query = f"SELECT TOP 100 {query} FROM {t} ORDER BY newid()"

            table_data = self.conn.execute(query)
            table_keys = list(table_data.keys())
            table_data = [list(x) for x in table_data]
            table_data_with_header = [table_keys] + table_data
            self.table_data[t] = table_data_with_header
            
            if table_data and len(table_data) > 0:
                table_sample_data = {}
                transposed_data = list(zip(*table_data))
                for idx, col in enumerate(table_keys):
                    column_data = transposed_data[idx]
                    S = Sampler(None, column_data)
                    table_sample_data[col] = S.get_sample_data()
                self.sample_data[t] = table_sample_data
            else:
                self.sample_data[t] = {}
        # connection.clear_output_converters()

    def get_insertion_order(self):
        tables = []
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
        self.get_user_defined_types()
        self.get_foreign_relations()
        self.get_insertion_order()
        self.get_table_keys()
        self.get_table_details()
        self.get_check_constraints()
        self.get_computed_columns()
        self.get_sample_data()
        self.get_master_tables()
        db_document = self.prepare_db_document()
        table_documents = self.prepare_table_document()

        # import json
        # json_data = json.dumps(table_documents, indent=4, sort_keys=True, default=str)
        # with open('adworks.json', 'w') as outfile:
        #     outfile.write(json_data)

        return db_document, table_documents