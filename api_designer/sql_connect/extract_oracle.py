import sys

from flask import Flask, jsonify
from sqlalchemy import create_engine, text
from api_designer.sql_connect.utils import *
from api_designer.sql_connect.ts2 import get_ts_order
from api_designer.sql_connect.oracle_decoder import DTDecoder
from api_designer.sql_connect.oracle_openapi import DTMapper
from api_designer.sql_connect.ezsampler import Sampler
import oracledb, re

class Extractor:
    def __init__(self, dbtype, args):
        self.dbtype = dbtype
        DIALECT = 'oracle'
        SQL_DRIVER = 'oracledb'
        ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + args["user"] + ':' + args["password"] +'@' + args["host"] + ':' + str(args["port"]) + '/?service_name=' + args["serviceName"]
        #ENGINE_PATH_WIN_AUTH = DIALECT + '://' + args["user"] + ':' + args["password"] + '@' + args["host"] + ':' + str(args["port"]) + '/?service_name=' + args["serviceName"]
        oracledb.version = "8.3.0"
        sys.modules["cx_Oracle"] = oracledb
        self.engine = create_engine(ENGINE_PATH_WIN_AUTH, thick_mode=None)
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
        query = text("""SELECT username FROM dba_users
                        WHERE account_status = 'OPEN' AND username not in ('SYS', 'SYSTEM')
                        ORDER BY username 
                    """)
        self.schemas = self.conn.execute(query)
        self.schemas = [x[0] for x in self.schemas]
    
    def get_tables(self):
        for s in self.schemas:
            query = text(f"""SELECT owner, table_name FROM dba_tables WHERE owner = '{s}'
                            ORDER BY owner, table_name
                        """)
            res = self.conn.execute(query)
            res = [f"{x[0]}.{x[1]}" for x in res]
            self.tables += res

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
    
    def get_table_size(self):
        for s in self.tables:
            query = text(f"SELECT COUNT(*) FROM {s}")
            # query = text("SELECT COUNT_BIG(*) FROM CONTACTS")
            res = self.conn.execute(query)
            res = next(res)
            self.table_size[s] = res[0]
    
    # Reference - https://stackoverflow.com/questions/1729996/list-of-foreign-keys-and-the-tables-they-reference-in-oracle-db
    def get_foreign_relations(self):
        query = text("""
            SELECT a.constraint_name FK_NAME, a.owner schema, a.table_name foreign_table, a.column_name fk_column, c.owner primary_schema, 
            c_pk.table_name primary_table,  b.column_name pk_column
            FROM user_cons_columns a
            JOIN user_constraints c ON a.owner = c.owner
                AND a.constraint_name = c.constraint_name
            JOIN user_constraints c_pk ON c.r_owner = c_pk.owner
                AND c.r_constraint_name = c_pk.constraint_name
            JOIN user_cons_columns b ON C_PK.owner = b.owner
                AND  C_PK.CONSTRAINT_NAME = b.constraint_name AND b.POSITION = a.POSITION     
            WHERE c.constraint_type = 'R'
        """)
        res = self.conn.execute(query)
        columns = list(res.keys())
        res = [x for x in res]

        for r in res:
            tmp = {x:y for x,y in zip(columns, r)}
            key = f"{tmp['schema']}.{tmp['foreign_table']}.{tmp['fk_column']}"
            self.foreign[key] = {
                'key': tmp['fk_name'],
                'schema': tmp['primary_schema'],
                'table': tmp['primary_table'],
                'column': tmp['pk_column']
            }

    def get_computed_columns(self):
        query = """
        select owner as schema_name,
            table_name,
            column_name,
            data_default as definition
        from sys.all_tab_cols
        where virtual_column != 'NO'
            and owner not in ('ANONYMOUS','CTXSYS','DBSNMP','EXFSYS', 'LBACSYS', 
            'MDSYS','MGMT_VIEW','OLAPSYS','OWBSYS','ORDPLUGINS','ORDSYS','OUTLN', 
            'SI_INFORMTN_SCHEMA','SYS','SYSMAN','SYSTEM','TSMSYS','WK_TEST',
            'WKSYS','WKPROXY','WMSYS','XDB','APEX_040000','APEX_PUBLIC_USER',
            'DIP','FLOWS_30000','FLOWS_FILES','MDDATA','ORACLE_OCM','XS$NULL',
            'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR', 'PUBLIC')
            and data_default is not null
        order by schema_name,
                table_name,
                column_name
        """
        query = text(query)
        res = self.conn.execute(query)
        column_keys = list(res.keys())
        res = [x for x in res]

        for r in res:
            tmp = {x:y for x,y in zip(column_keys, r)}
            key = f"{tmp['schema_name']}.{tmp['table_name']}"
            if key not in self.computed_columns:
                self.computed_columns[key] = {}
            self.computed_columns[key][tmp['column_name']] = tmp['definition']

    def get_table_keys(self):
        for s in self.schemas:
            query = text(f"""
                select
                all_cons_columns.owner as schema_name,
                all_cons_columns.table_name, 
                all_cons_columns.column_name, 
                all_cons_columns.position, 
                all_constraints.status
                from all_constraints, all_cons_columns 
                where 
                all_constraints.constraint_type = 'P'
                and all_constraints.constraint_name = all_cons_columns.constraint_name
                and all_constraints.owner = all_cons_columns.owner and all_constraints.owner = '{s}'
                order by 
                all_cons_columns.owner,
                all_cons_columns.table_name, 
                all_cons_columns.position
            """)
            res = self.conn.execute(query)
            columns = list(res.keys())
            res = [x for x in res]

            for r in res:
                tmp = {x:y for x,y in zip(columns, r)}
                key = f"{s}.{tmp['table_name']}"
                if key not in self.table_keys:
                    self.table_keys[key] = []
                self.table_keys[key].append(tmp['column_name'])

    @staticmethod
    def decode_table_attributes(data):
        data = {
            'datatype': str(data['data_type']),
            'valueconstraint': 'null' if data['nullable'] == 'YES' else 'not null',
            'default': data['data_default']
        }
        data['auto'] = True if data['default'] else False
        data['serial'] = True if (data['default'] and '.nextval' in data['default']) else False
        return data

        

    def get_check_constraints(self):
        query = text("""
        select tab.owner as schema_name,
            tab.table_name,
            con.constraint_name,
            cols.column_name,
            search_condition as definition,
            con.status
        from sys.all_tables tab
        join sys.all_constraints con
            on tab.owner = con.owner
            and tab.table_name = con.table_name
        join sys.all_cons_columns cols
            on cols.owner = con.owner
            and cols.constraint_name = con.constraint_name
            and cols.table_name = con.table_name
        where constraint_type = 'C'
            and tab.owner not in ('ANONYMOUS','CTXSYS','DBSNMP','EXFSYS',
            'LBACSYS', 'MDSYS', 'MGMT_VIEW','OLAPSYS','OWBSYS','ORDPLUGINS',
            'ORDSYS','OUTLN', 'SI_INFORMTN_SCHEMA','SYS','SYSMAN','SYSTEM',
            'TSMSYS','WK_TEST','WKSYS', 'WKPROXY','WMSYS','XDB','APEX_040000',
            'APEX_040200', 'APEX_PUBLIC_USER', 'DIP', 'FLOWS_30000','FLOWS_FILES',
            'MDDATA', 'ORACLE_OCM', 'XS$NULL', 'SPATIAL_CSW_ADMIN_USR', 
            'SPATIAL_WFS_ADMIN_USR', 'PUBLIC')
        order by tab.owner,
                tab.table_name,
                cols.position
        """)
        res = self.conn.execute(query)
        column_keys = list(res.keys())
        res = [x for x in res]

        for r in res:
            tmp = {x:y for x,y in zip(column_keys, r)}
            key = tmp['schema_name'] + '.' + tmp['table_name']
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

    def get_table_details(self):
        for t in self.tables:
            t_schema, t_name = t.split(".")
            if "[" in t_name :
                t_name = t_name.replace("[","").replace("]","")
            query = text(f"""SELECT * FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '{t_name}'
                            """)
            column_desc = self.conn.execute(query)
            column_keys = column_desc.keys()
            column_desc = list(column_desc)

            attributes = {}
            for cd in column_desc:
                tmp = {x:y for x, y in zip(column_keys, cd)}
                # attributes[tmp['COLUMN_NAME']] = {}
                attributes[tmp['column_name']] = Extractor.decode_table_attributes(tmp)

                D = DTDecoder(tmp)
                attributes[tmp['column_name']]['decoder'] = D.decoder()
                
                D = DTMapper(tmp, self.user_defined_types)
                attributes[tmp['column_name']]['openapi'] = D.decoder()
            self.table_details[t] = attributes

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
                    if len(self.sample_data[tk]) > 0:
                        # col = col.lower()
                        col_sample = self.sample_data[tk][col]
                        # col = col.upper()
                        col_details = self.table_details[tk][col]['decoder']
                        if (col_sample.get('repeat') != 1) or (col_details.get("type") not in ("number", "string")):
                            master = False

                if master:
                    self.master_tables.append(tk)

    def get_sample_data(self):
        for t in self.tables:
            q = t.split(".")[1]
            query = text(f""" SELECT *
                        FROM (
                        SELECT *
                        FROM "{q}"
                        ORDER BY DBMS_RANDOM.VALUE
                        )
                        WHERE ROWNUM <= 100
                    """)
            table_data = self.conn.execute(query)
            table_keys = list(table_data.keys())
            list_of_columns = [each_col.upper() for each_col in table_keys]
            table_data = [list(x) for x in table_data]
            table_data_with_header = [list_of_columns] + table_data
            self.table_data[t] = table_data_with_header
            
            table_sample_data = {}
            if table_data and len(table_data) > 0:
                transposed_data = list(zip(*table_data))
                for idx, col in enumerate(list_of_columns):
                    column_data = transposed_data[idx]
                    S = Sampler(None, column_data)
                    table_sample_data[col] = S.get_sample_data()
                self.sample_data[t] = table_sample_data
            else:
                self.sample_data[t] = {}

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
        self.get_computed_columns()
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