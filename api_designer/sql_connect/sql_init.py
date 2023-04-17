from email.policy import default
from api_designer.mongo import store_document, store_bulk_document
from api_designer.sql_connect.extract_postgres import Extractor as PsqlExtractor
from api_designer.sql_connect.extract_mysql import Extractor as MysqlExtractor
from api_designer.sql_connect.extract_oracle import Extractor as OracleExtractor
from api_designer.sql_connect.extract_mssql import Extractor as MssqlExtractor
from api_designer.utils.decrypter import _decrypt

from google.cloud import storage
from google.oauth2 import service_account
from urllib.parse import urlparse
import json, os, re, shutil, subprocess
from decouple import config

MSSQL_DEFAULT_PORT = 1433
POSTGRES_DEFAULT_PORT = 5432

def download_gcsfile(url):
    creds = service_account.Credentials.from_service_account_file('creds.json')
    storage_client = storage.Client(credentials=creds)
    bucket, file_path = decode_gcs_url(url)
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(file_path)
    destfilePath = "gcpdownloads/"+os.path.basename(file_path)
    blob.download_to_filename(destfilePath)
    generate_code(file_path)
    return destfilePath

def decode_gcs_url(url):
    p = urlparse(url)
    path = p.path[1:].split('/', 1)
    bucket, file_path = path[0], path[1]
    return bucket, file_path

def generate_code(file_path):
    cmd1 = "cd gcpdownloads/"
    cmd2 = "chmod 600 " + file_path.split('/')[1]
    cmd = cmd1 + "; " + cmd2
    cmd3 = "pwd"

    subprocess.run(cmd, shell=True)

def handle_sql_connect(request_data, dbtype, projectid, db):
    if config("DEVELOPMENT", default = False):
        server = str(config("server"))
        username = str(config("username2"))
        password = str(config("password"))
        database = str(config("database"))
        portNo = config("portNo", default="")

        if config("ssl"):
            certPath = str(config("certPath"))
            keyPath = str(config("keyPath"))
            rootPath = str(config("rootPath"))
            sslMode = str(config("sslMode"))


    else:
        passkey = config('dbpasskey', default = None)
        server = str(request_data.get("server", ""))
        username = str(request_data.get("username", ""))
        password = request_data.get("password", "")
        database = str(request_data.get("database", ""))
        portNo = request_data.get("portNo", "")
        certPath = str(request_data.get("certPath", ""))
        keyPath = str(request_data.get("keyPath", ""))
        rootPath = str(request_data.get("rootPath", ""))
        sslMode = str(request_data.get("sslMode", ""))

        if keyPath:
            keyPath = download_gcsfile(keyPath)
        if rootPath:
            rootPath = download_gcsfile(rootPath)
        if certPath:
            certPath = download_gcsfile(certPath)
        if password and passkey:
            decryptedpassword = _decrypt(bytes(password, 'utf-8'), "", passkey)
            password = bytes.decode(decryptedpassword)

    if dbtype == "postgres":
        if not portNo:
            portNo = POSTGRES_DEFAULT_PORT
            
        if sslMode == "Y":
            # keyPath and rootPath and certPath:
            #print("server", server)
            #print("username", username)
            #print("database", database)
            ## add code to download the files from the gc bucket path to local uploads folder
            args = {
                "host": server,
                "user": username,
                "database": database,
                "sslcert": certPath,
                "sslkey": keyPath,
                "sslrootcert": rootPath,
                "sslmode": "verify-full"
            }
            #print("args", args)
        else:
            args = {
                "host": server,
                "user": username,
                "database": database,
                "password": password,
                "port": portNo
            }

        P = PsqlExtractor(dbtype, args)
        db_document, table_documents = P.extract_data(projectid)

        if db_document and table_documents:
            store_document("database", db_document, db)
            store_bulk_document("tables", table_documents, db)
            return {"success": True}
        else:
            return {"success": False}

    elif dbtype == "oracle":
        if not portNo:
            portNo = POSTGRES_DEFAULT_PORT

        if database:
            args = {
                "host": server,
                "user": username,
                "serviceName": database,
                "password": password,
                "port": portNo
            }

        P = OracleExtractor(dbtype, args)
        db_document, table_documents = P.extract_data(projectid)
        # db_document, table_documents = P.extract_data(projectid)
        # P.get_schemas()
        # P.get_tables()

        if db_document and table_documents:
            store_document("database", db_document, db)
            store_bulk_document("tables", table_documents, db)
            return {"success": True}
        else:
            return {"success": False}

    elif dbtype == "mysql":
        args = {
            "host": "xxxx",
            "database": "xxxxx",
            "user": "xxxx",
            "password": "xxxxx"
        }
        #P = MysqlExtractor(args)
        #res = P.extract_data()
        #print(res)

    elif dbtype == "mssql":
        if not portNo:
            portNo = MSSQL_DEFAULT_PORT

        from sqlalchemy.engine import URL
        connection_url = URL.create(
            "mssql+pyodbc",
            username=username,
            password=password,
            host=server,
            database=database,
            port=portNo,
            query={
                "driver": "ODBC Driver 17 for SQL Server"
            },
        )
        P = MssqlExtractor(dbtype, connection_url)
        #P.extract_sp(projectid)
        db_document, table_documents, sp_docs, dbdata_map_documents = P.extract_data(projectid)
        #print("db_document", db_document)
        #print("table_documents", table_documents)
        store_document("database", db_document, db)
        store_bulk_document("tables", table_documents, db)
        if sp_docs:
            store_bulk_document("stored_procedures", sp_docs, db)
        if dbdata_map_documents:
            store_bulk_document("table_dbdata_map", dbdata_map_documents, db)

        return {"success": True}