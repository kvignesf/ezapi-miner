from sqlalchemy import column, false, null, true
from api_designer import mongo

from api_designer.raw_spec_parser.openapi_parser import parse_openapi
from api_designer.raw_spec_parser.swagger_parser import parse_swagger

from api_designer.utils.common import *


def parse_raw_spec(projectid, db):   
    spec_orig = db.raw_spec.find_one({"projectid": projectid})
    spec_filename = spec_orig["filename"]
    jsondata = spec_orig['data']

    if "openapi" in jsondata:
        pass
    elif "swagger" in jsondata:
        pass

    version = None
    if "swagger" in jsondata:
        version = jsondata.get("swagger")
    elif "openapi" in jsondata:
        version = jsondata.get("openapi")

    if version[0] == "2":
        res = parse_swagger(jsondata, projectid, db)
        return res

    elif version[0] == "3":
        res = parse_openapi(jsondata, projectid, spec_filename, db)
        return res

    else:
        res = {
            "success": False,
            "message": "Spec version is not specified",
            "status": 500,
        }
    return res



    