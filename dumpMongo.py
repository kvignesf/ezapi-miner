import shutil
import subprocess
import threading


def dumpDB(dbname):
    print("Inside dump collection")
    connectionString = "mongodb://localhost:27017/" + dbname

    output = dbname

    # archive and out cannot be used simultaenously
    # cmd = f"mongodump --uri={connectionString} --archive={output} --out={out}"

    cmd = f"mongodump --uri={connectionString} --archive={output}"
    subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)

    source = output
    destination = "/Users/shbham/Desktop/ezapi/dumps"

    shutil.move(source, destination)
    print("database dumped successfully to destination folder")


def dumpData(dbname):
    thread = threading.Thread(target=dumpDB, args=(dbname,))
    thread.start()

# dumpCollection("mongodb://localhost:27017/ezapi")
