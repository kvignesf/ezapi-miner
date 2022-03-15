# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************


from api_designer import config

from api_designer.spec_parser.parser_init import parse_openapi_json
from api_designer.ddl_parser.parser_init import parse_ddl_file
from api_designer.matcher.matcher_init import spec_ddl_matcher
from api_designer.spec_generator.generator_init import generate_spec
from api_designer.artefacts.artefacts_init import generate_artefacts
from api_designer.visualizer.sankey import process_sankey_visualizer
from api_designer.sql_connect.sql_init import handle_sql_connect
# from api_designer.codegen.jdl_init import generate_jdl_file
# from api_designer.ddl_parser.parser_init import parse_db_ddl_file
# from api_designer.ddl_parser.parser_init import gen_db_ddl_file


class EzAPIModels:
    def __init__(self, projectid):
        self.projectid = projectid

    def set_db_instance(self):
        client, db = config.get_db_connection()
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

    def artefacts_geerator(self):
        ret = generate_artefacts(self.projectid, self.db)
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