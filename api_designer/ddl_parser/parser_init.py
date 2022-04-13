from api_designer import mongo
from api_designer.ddl_parser.mssql_parser import Parser as MSSqlParser
from api_designer.ddl_parser.postgresql_parser import Parser as PostgresqlParser
from api_designer.ddl_parser.mysql_parser import Parser as MysqlParser


TABLE_COLLECTION = "tables"

def parse_ddl_file(ddl_file, projectid, ddl_filename, db, ddltype = 'mssql'):
    file = open(ddl_file, "r+")
    filedata = file.readlines()

    if ddltype == 'mssql':
        P = MSSqlParser(filedata)

    elif ddltype == 'mysql':
        P = MysqlParser(filedata)

    elif ddltype == 'postgres':
        P = PostgresqlParser(filedata)


    parsed_result = P.parse_data()
    if parsed_result:
        for table in parsed_result:
            table_document = table
            table_document["projectid"] = projectid
            table_document["ddl_file"] = ddl_filename
            mongo.store_document(TABLE_COLLECTION, table_document, db)

        #return {"success": True, "status": 200, "message": "ok", "data": parsed_result}
        return {"success": True, "status": 200, "message": "ok"}
    else:
        return {"success": False, "status": 500, "message": "Error parsing file data"}

