import subprocess


def dumpCollection(connectionString):
    output = "testdump"
    cmd = f"mongodump --uri={connectionString} --archive={output}"
    subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)

# dumpCollection("mongodb://localhost:27017/ezapi")
