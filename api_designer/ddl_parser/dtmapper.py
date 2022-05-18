# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************


_OPENAPI_FORMAT_MAPPER = {
    "int32": "integer",
    "int64": "integer",
    "float": "number",
    "double": "number",
    "byte": "string",
    "binary": "string",
    "date": "string",
    "date-time": "string",
}

# openapi format -> sql datatype
_DT_MSSQL_MAPPER = {
    "int32": ["tinyint", "smallint", "int"],
    "int64": ["bigint"],
    "float": ["smallmoney", "money", "float", "real"],
    "double": ["decimal", "numeric"],
    "byte": ["bit", "mediumtext", "mediumblob"],
    "binary": ["binary", "varbinary", "varbinary", "image"],
    "date": ["date"],
    "date-time": ["datetime", "datetime2", "smalldatetime", "datetimeoffset"],
}

_DT_MYSQL_MAPPER = {
    "int32": ["tinyint", "smallint", "int", "integer"],
    "int64": ["bigint"],
    "float": ["smallmoney", "money", "float", "real"],
    "double": ["decimal", "numeric"],
    "byte": ["bit"],
    "binary": ["binary", "varbinary", "varbinary", "image"],
    "date": ["date"],
    "date-time": ["datetime", "datetime2", "smalldatetime", "datetimeoffset"],
}

_DT_POSTGRES_MAPPER = {
    "int32": ["tinyint", "smallint", "int", "integer"],
    "int64": ["bigint"],
    "float": ["smallmoney", "money", "float", "real"],
    "double": ["decimal", "numeric"],
    "byte": ["bit"],
    "binary": ["binary", "varbinary", "varbinary", "image"],
    "date": ["date"],
    "date-time": ["datetime", "datetime2", "smalldatetime", "datetimeoffset"],
}

# Rest - type - string, format - sql_server_{dttype}


def convert_sql_server_dtype(dtype):
    ret = {"type": None, "format": None}

    found = False
    for k, v in _DT_MSSQL_MAPPER.items():
        if dtype in v:
            found = True
            ret["type"] = _OPENAPI_FORMAT_MAPPER[k]
            ret["format"] = k

    if not found:
        ret["type"] = "string"
        ret["format"] = "sql_server_" + dtype

    return ret


def convert_mysql_server_dtype(dtype):
    ret = {"type": None, "format": None}

    found = False
    for k, v in _DT_MYSQL_MAPPER.items():
        if dtype in v:
            found = True
            ret["type"] = _OPENAPI_FORMAT_MAPPER[k]
            ret["format"] = k

    if not found:
        ret["type"] = "string"
        ret["format"] = "mysql_server_" + dtype

    return ret


def convert_postgres_server_dtype(dtype):
    ret = {"type": None, "format": None}

    found = False
    for k, v in _DT_POSTGRES_MAPPER.items():
        if dtype in v:
            found = True
            ret["type"] = _OPENAPI_FORMAT_MAPPER[k]
            ret["format"] = k

    if not found:
        ret["type"] = "string"
        ret["format"] = "postgres_server_" + dtype

    return ret
