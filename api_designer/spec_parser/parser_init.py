from api_designer import config

from api_designer.spec_parser.openapi_parser import parse_openapi
from api_designer.spec_parser.swagger_parser import parse_swagger

import codecs
import json
import os, sys


def parse_openapi_json(filepath, api_design_id, spec_filename, db):
    try:
        jsondata = json.load(codecs.open(filepath, "r", "utf-8-sig"))
    except Exception as e:
        res = {
            "success": False,
            "errorType": type(e).__name__,
            "error": str(e),
            "message": "Some error has occured in parsing data",
            "status": 500,
        }
        return res

    try:
        assert isinstance(jsondata, dict)
    except Exception as e:
        res = {
            "success": False,
            "errorType": type(e).__name__,
            "error": str(e),
            "message": "Some error has occured in parsing data",
            "status": 500,
        }
        return res

    jsonstr = json.dumps(jsondata)
    jsonstr = jsonstr.replace("$ref", "ezapi_ref")
    jsondata = json.loads(jsonstr)

    version = None
    if "swagger" in jsondata:
        version = jsondata.get("swagger")
    elif "openapi" in jsondata:
        version = jsondata.get("openapi")

    if version[0] == "2":
        res = parse_swagger(jsondata, api_design_id, spec_filename, db)
        return res

    elif version[0] == "3":
        res = parse_openapi(jsondata, api_design_id, spec_filename, db)
        return res

    else:
        res = {
            "success": False,
            "message": "Spec version is not specified",
            "status": 500,
        }
        return res
