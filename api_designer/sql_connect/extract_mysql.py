from sqlalchemy import create_engine

class Extractor:
    def __init__(self, args):
        self.engine = create_engine('mysql+pymysql://', connect_args = args)
        self.conn = self.engine.connect()

    def extract_data(self):
        self.tables = self.conn.execute("show tables")
        self.tables = [x[0] for x in self.tables]
        return self.tables