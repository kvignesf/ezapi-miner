from api_designer import config

from api_designer.spec_parser.parser_init import parse_openapi_json
from api_designer.ddl_parser.parser_init import parse_ddl_file
from api_designer.matcher.matcher_init import spec_ddl_matcher


class EzAPIModels:
    def __init__(self, projectid):
        self.projectid = projectid

    def set_db_instance(self):
        client, db = config.get_db_connection()
        self.client = client
        self.db = db

    def parse_spec_file(self, spec_path, spec_filename):
        ret = parse_openapi_json(spec_path, self.projectid, spec_filename, self.db)
        return ret

    def parse_ddl_file(self, ddl_path, ddl_filename):
        ret = parse_ddl_file(ddl_path, self.projectid, ddl_filename, self.db)
        return ret

    def matcher(self):
        ret = spec_ddl_matcher(self.projectid, self.db)
        return ret
