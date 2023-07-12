# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************


from api_designer import mongo

from api_designer.spec_parser.parser_init import parse_openapi_json
from api_designer.ddl_parser.parser_init import parse_ddl_file
from api_designer.matcher.matcher_init import spec_ddl_matcher
from api_designer.spec_generator.generator_init import generate_spec
from api_designer.artefacts.artefacts_init import generate_artefacts, generate_simulation_artefacts
from api_designer.artefacts.artefacts_init import check_keywrd_exists
from api_designer.raw_spec_parser.parser_init import parse_raw_spec

from api_designer.visualizer.sankey import process_sankey_visualizer
from api_designer.sql_connect.sql_init import handle_sql_connect
from api_designer.codegen.jdl_init import generate_jdl_file
from api_designer.artefacts2.init import TestdataGenerator
from api_designer.artefacts2.insert_placeholders import TestdataUpdator
from api_designer.artefacts2.mongo_init import TestdataGenerator as MongoTestGenerator
# from api_designer.ddl_parser.parser_init import parse_db_ddl_file
# from api_designer.ddl_parser.parser_init import gen_db_ddl_file
from api_designer.nosql_connect.nosql_init import handle_nosql_connect

class BaseEzapiModels:
    def __init__(self, keywrd):
        self.keywrd = keywrd

    def set_db_instance(self):
        client, db = mongo.get_db_connection()
        self.client = client
        self.db = db
        # self.client = None
        # self.db = None

    def check_keywrd(self, dbtype):
        ret = check_keywrd_exists(self.keywrd, self.db, dbtype)
        return ret

class EzAPIModels:
    def __init__(self, projectid):
        self.projectid = projectid

    def set_db_instance(self):
        client, db = mongo.get_db_connection()
        self.client = client
        self.db = db
        # self.client = None
        # self.db = None

    def parse_spec_file(self, spec_path, spec_filename):
        ret = parse_openapi_json(spec_path, self.projectid, spec_filename, self.db)
        return ret

    def parse_ddl_file(self, ddl_path, ddl_filename, ddltype = "mssql"):
        ret = parse_ddl_file(ddl_path, self.projectid, ddl_filename, self.db, ddltype)
        return ret

    # def parse_db_ddl_file(self, ddl_path, ddl_filename, db_type):
    #     ret = parse_db_ddl_file(ddl_path, self.projectid, ddl_filename, db_type, self.db)
    #     return ret

    # def gen_db_ddl_file(self, server, username, password, database):
    #     ret = gen_db_ddl_file(server, username, password, database, self.projectid, self.db)
    #     return ret

    def matcher(self):
        ret = spec_ddl_matcher(self.projectid, self.db)
        return ret

    def spec_generator(self):
        ret = generate_spec(self.projectid, self.db)
        return ret

    def raw_spec_parser(self):
        ret = parse_raw_spec(self.projectid, self.db)
        return ret

    def artefacts_generator(self):
        ret = generate_artefacts(self.projectid, self.db)
        return ret

    def sim_artefacts_generator(self, operationId):
        ret = generate_simulation_artefacts(self.projectid, self.db, operationId)
        return ret

    def sankey_generator(self):
        ret = process_sankey_visualizer(self.projectid, self.db)
        return ret

    def jdl_generator(self):
        ret = generate_jdl_file(self.projectid, self.db)
        return ret

    def extract_sql_connect(self, request_data, dbtype):
        ret = handle_sql_connect(request_data, dbtype, self.projectid, self.db)
        return ret

    def artefacts_generator2(self, type):
        TG = TestdataGenerator(self.projectid, self.db, type)
        TG.fetch_data()
        return TG.generate_testdata()

    def update_testdata(self):
        TU = TestdataUpdator(self.projectid, self.db)
        TU.fetch_data()
        return TU.update_testcases()

    def extract_sp_data(self, request_data, dbtype):
        ret = handle_sql_connect(request_data, dbtype, self.projectid, self.db)
        return ret

    def extract_nosql_connect(self, request_data, dbtype):
        ret = handle_nosql_connect(request_data, dbtype, self.projectid, self.db)
        return ret

    def artefacts_generator_mongo(self, type):
        MTG = MongoTestGenerator(self.projectid, self.db, type)
        MTG.fetch_data()
        return MTG.generate_testdata()
