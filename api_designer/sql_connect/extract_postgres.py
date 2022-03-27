from pprint import pprint
import psycopg2
from random import sample
from sqlalchemy import create_engine

from api_designer.sql_connect.ts import get_ts_order
from api_designer.sql_connect.postgres_decoder import DTDecoder
from api_designer.sql_connect.ezsampler import Sampler

_SYSTEM_SCHEMAS = ['pg_toast', 'pg_catalog', 'information_schema']

class Extractor:
    def __init__(self, args):
        self.engine = create_engine('postgresql+psycopg2://', connect_args = args)
        self.conn = self.engine.connect()

        self.schemas = []
        self.tables = []
        self.table_size = {}
        self.table_keys = {}
        self.foreign = {}
        self.table_details = {}
        self.insertion_order = None
        self.sample_data = {}
        self.master_tables = []

    def get_schemas(self):
        self.schemas = self.conn.execute("select schema_name from information_schema.schemata")
        self.schemas = [x[0] for x in self.schemas if x[0] not in _SYSTEM_SCHEMAS]

    def get_tables(self):
        for s in self.schemas:
            res = self.conn.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{s}'")
            res = [f"{s}.{x[0]}" for x in res]
            self.tables += res

    def get_table_size(self):
        for s in self.tables:
            res = self.conn.execute(f"SELECT COUNT(*) FROM {s}")
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
        res = self.conn.execute(query)
        columns = list(res.keys())
        res = [x for x in res]

        for r in res:
            tmp = {x:y for x,y in zip(columns, r)}
            key = f"{tmp['foreign_table']}.{tmp['fk_column']}"
            value = f"{tmp['primary_table']}.{tmp['pk_column']}"
            self.foreign[key] = value

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
            res = self.conn.execute(query)
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
        for tk, tv in self.table_details.items():
            table_size = self.table_size[tk]
            table_columns = 0
            table_foreign = 0
            column_names = []
            for col, col_data in tv.items():
                foreign_key = f"{tk}.{col}"
                if col_data["auto"]:
                    continue
                elif foreign_key in self.foreign:
                    table_foreign += 1
                else:
                    table_columns += 1
                    column_names.append(col)

            if (table_foreign + table_columns) <= 3 and table_foreign <= 1 and table_columns> 0 and table_size <= 150:
                master = True
                for col in column_names:
                    col_sample = self.sample_data[tk][col]
                    col_details = self.table_details[tk][col]['decoder']
                    if (col_sample.get('repeat') != 1) or (col_details.get("type") not in ("number", "string")):
                        master = False

                if master:
                    self.master_tables.append(tk)

    def get_master_tables2(self):
        for tk, tv in self.table_details.items():
            table_columns = 0
            column_name = None
            for col, col_data in tv.items():
                foreign_key = f"{tk}.{col}"
                if col_data["auto"] or foreign_key in self.foreign:
                    continue
                else:
                    table_columns += 1
                    column_name = col

            if table_columns == 1:
                column_sample = self.sample_data[tk][column_name]
                if column_sample['repeat'] == 1:
                    self.master_tables.append(tk)

    def get_table_details(self):
        for t in self.tables:
            t_schema, t_name = t.split(".")
            column_desc = self.conn.execute(f"SELECT * from information_schema.columns where table_schema='{t_schema}' and table_name = '{t_name}'")
            column_keys = column_desc.keys()
            column_desc = list(column_desc)
            
            attributes = {}

            for cd in column_desc:
                tmp = {x:y for x, y in zip(column_keys, cd)}
                attributes[tmp['column_name']] = Extractor.decode_table_attributes(tmp)

                D = DTDecoder(tmp)
                attributes[tmp['column_name']]['decoder'] = D.decoder()
            self.table_details[t] = attributes
            
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
            table_data = self.conn.execute(f"select * from {t} order by random() limit 100")
            table_keys = list(table_data.keys())
            table_data = [list(x) for x in table_data]
            
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
        for t in self.tables:
            foreign_dependencies = set()

            for ft, fr in self.foreign.items():
                if t == ft.rsplit(".", 1)[0]:
                    tmp = fr.rsplit(".", 1)[0]
                    foreign_dependencies.add(tmp)
            tables.append({
                "key": t,
                "dependencies": list(foreign_dependencies)
            })
        self.insertion_order = get_ts_order(tables)

    def prepare_db_document(self):
        document = {
            'projectid': self.projectid,
            'type': 'postgres',
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
                'attributes': []
            }

            for col, col_data in self.table_details[t].items():
                col_data['name'] = col
                col_data['keyType'] = keyType if col in keys else None
                
                foreign_key = f"{t}.{col}"
                if foreign_key in self.foreign:
                    tmp2 = self.foreign[foreign_key].split('.')
                    col_data['foreign'] = {
                        'schema': tmp2[0],
                        'table': tmp2[1],
                        'column': tmp2[2]
                    }
                if len(self.sample_data[t]) > 0:
                    col_data["sample"] = self.sample_data[t][col]
                document['attributes'].append(col_data)
            table_documents.append(document)
        return table_documents

    def extract_data(self, projectid):
        self.projectid = projectid
        self.get_schemas()
        self.get_tables()
        self.get_table_size()
        self.get_foreign_relations()
        self.get_insertion_order()
        self.get_table_keys()
        self.get_table_details()
        self.get_sample_data()
        self.get_master_tables()
        db_document = self.prepare_db_document()
        table_documents = self.prepare_table_document()

        # import json
        # json_data = json.dumps(table_documents, indent=4, sort_keys=True, default=str)
        # with open('dvdrental.json', 'w') as outfile:
        #     outfile.write(json_data)

        return db_document, table_documents