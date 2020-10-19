# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************

from apiops import config

from apiops.parser.swagger_parser import parse_swagger_api
from apiops.scorer.element_scorer import score_param_elements
from apiops.visualizer.sankey_generator import process_sankey_visualizer
from apiops.tester.test_generator import process_test_generator


class APIOPSModel:
    def __init__(self, filepath, filename, api_ops_id, dbname):
        self.filepath = filepath
        self.filename = filename
        self.dbname = dbname
        self.api_ops_id = api_ops_id

    def get_db_instance(self):
        client, db = config.get_db_connection(self.dbname)
        self.client = client
        self.db = db

    def parse_swagger_specs(self):
        res = parse_swagger_api(
            self.filepath, self.filename, self.api_ops_id, self.db)
        return res

    def score_request_elements(self):
        res = score_param_elements(self.api_ops_id, self.db)
        return res

    def prepare_sankey_data(self):
        res = process_sankey_visualizer(self.api_ops_id, self.db)
        return res

    def generate_test_data(self):
        res = process_test_generator(self.api_ops_id, self.db)
        return res
