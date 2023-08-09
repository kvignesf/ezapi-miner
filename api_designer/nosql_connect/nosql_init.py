from api_designer.mongo import store_document, store_bulk_document
from decouple import config
from api_designer.nosql_connect.extract_mongo import MongoExtractor
from urllib.parse import urlparse
import json, os, re, shutil, subprocess
from decouple import config
from google.cloud import storage
from google.oauth2 import service_account
from api_designer.utils.decrypter import _decrypt

from pprint import pprint
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

def handle_nosql_connect(request_data, dbtype, projectid, db):
    uri = dbname = None
    if config("DEVELOPMENT", default = False):
        uri = str(config("mongo_connection_uri"))
        dbname = str(config("mongo_db_name"))
    else:
        #mongodb://root:JRVvuh9D5V0IZxCW@34.66.45.162:27017
        authdb = request_data.get("authdb", "")
        passkey = config('dbpasskey', default=None)
        server = str(request_data.get("server", ""))
        username = str(request_data.get("username", ""))
        password = request_data.get("password", "")
        dbname = str(request_data.get("database", ""))
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

    ipAddressRegex = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b')
    if ipAddressRegex.match(server) and not portNo:
        portNo = "27017";
    elif server.__contains__(".mongodb.net"):
        portNo = "";

    if portNo:
        uri = "mongodb://" + username + ":" + password + "@" + server + ":" + portNo
        if authdb:
            uri = "mongodb://"+username+":"+password+"@"+server+":"+portNo+"/?authSource="+dbname

    else:
        uri = "mongodb+srv://"+username+":"+password+"@"+server+"/?ssl=true&ssl_cert_reqs=CERT_NONE"

    print(uri, dbname)
    P = MongoExtractor("mongo", uri, dbname)
    #collection_schemas = P.extract_data(projectid)
    db_document, collection_document = P.extract_data(projectid)

    # pprint(collection_schemas)
    
    # import json
    # with open("sample_airbnb.json", "w") as outfile:
    #     outfile.write(json.dumps(collection_schemas, indent=4))

    if db_document and collection_document:
        store_document("database", db_document, db)
        store_bulk_document("mongo_collections", collection_document, db)

    return {"success": True}