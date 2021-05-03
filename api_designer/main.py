from api_designer import config

from api_designer.spec_parser.parser_init import parse_openapi_json
from api_designer.ddl_parser.parser_init import parse_ddl_file
from api_designer.matcher.matcher_init import spec_ddl_matcher


class APIDesignModel:
    def __init__(self, spec_file, spec_filename, ddl_file, ddl_filename, api_design_id):
        self.spec_file = spec_file
        self.spec_filename = spec_filename
        self.ddl_file = ddl_file
        self.ddl_filename = ddl_filename
        self.api_design_id = api_design_id

    def get_db_instance(self):
        print("In db instance")
        client, db = config.get_db_connection()
        self.client = client
        self.db = db

    def parse_spec_file(self):
        res = parse_openapi_json(
            self.spec_file, self.api_design_id, self.spec_filename, self.db
        )
        return res

    def parse_ddl_file(self):
        res = parse_ddl_file(
            self.ddl_file, self.api_design_id, self.ddl_filename, self.db
        )
        return res

    def matcher(self):
        res = spec_ddl_matcher(self.api_design_id, self.db)
        return res
