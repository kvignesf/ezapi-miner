from api_designer.config import store_document, store_bulk_document
from api_designer.sql_connect.extract_postgres import Extractor as PsqlExtractor
from google.cloud import storage
from google.oauth2 import service_account
from urllib.parse import urlparse
import os
from api_designer.utils.decrypter import _decrypt
import json, re, shutil, subprocess
#from api_designer.sql_connect.extract_mysql import Extractor as MysqlExtractor

#from api_designer.sql_connect.generator import DataGen

def download_gcsfile(url):
    #url = str(request.form.get("url", ""))
    creds = service_account.Credentials.from_service_account_file('creds.json')

    #print("url", url)
    #print("creds", creds)

    storage_client = storage.Client(credentials=creds)
    bucket, file_path = decode_gcs_url(url)
    bucket = storage_client.bucket(bucket)
    print("file_path", file_path)
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

def handle_sql_connect(request_data, dbtype, projectid, db ):
    passkey = 'ezapidbpwdhandshake'
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
        keyPathLocal = download_gcsfile(keyPath)
    if rootPath:
        rootPathLocal = download_gcsfile(rootPath)
    if certPath:
        certPathLocal = download_gcsfile(certPath)
    if password:
        decryptedpassword = _decrypt(bytes(password, 'utf-8'), "", passkey)

    if dbtype == "postgres":
        if sslMode == "Y":
            # keyPath and rootPath and certPath:

            ## add code to download the files from the gc bucket path to local uploads folder
            args = {
                "host": server,
                "user": username,
                "database": database,
                "sslcert": certPathLocal,
                "sslkey": keyPathLocal,
                "sslrootcert": rootPathLocal,
                "sslmode": "verify-full"
            }
        else:
            args = {
                "host": server,
                "user": username,
                "database": database,
                "password": bytes.decode(decryptedpassword),
                "port": portNo
            }

        P = PsqlExtractor(args, sslMode)
        db_document, table_documents = P.extract_data()
        #from pprint import pprint
        #pprint(table_documents)
        
        # store_document("database", db_document, db)
        # store_bulk_document("tables", table_documents, db)
        if db_document and table_documents:
            return {"success": True}
        else:
            return {"success": False}

        # G = DataGen(table_documents)
        # G.gen()
        #return {"success": True}


    elif dbtype == "mysql":
        args = {
            "host": "database-1.c9jpouik3ypb.ap-south-1.rds.amazonaws.com",
            "database": "testezapi",
            "user": "admin1",
            "password": "welcome2cumulations"
        }
        #P = MysqlExtractor(args)
        #res = P.extract_data()
        #print(res)
    elif dbtype == "mssql":
        pass