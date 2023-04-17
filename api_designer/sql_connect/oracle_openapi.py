_OPENAPI_FORMAT_MAPPER = {
    "int32": "integer",
    "int64": "integer",
    "float": "number",
    "double": "number",
    "byte": "string",
    "binary": "string",
    "date": "string",
    "date-time": "string",
    "boolean": "boolean"
}

_DT_MAPPER = {
    "int32": ["smallint", "integer", "smallserial", "serial"],
    "int64": ["bigint", "bigserial"],
    "float": ["money", "real"],
    "double": ["decimal", "numeric", "double precision"],
    "boolean": ["boolean"],
    "byte": ["bit"],
    "binary": ["bytea", "bit varying"],
    "date": ["date"],
    "date-time": ["datetime", "datetime2", "smalldatetime", "datetimeoffset"],
}

class DTMapper:
    def __init__(self, props, udt):
        self.props = props
        self.udt = udt

    def decoder(self):
        ret = {"type": None, "format": None}
        dtype = self.props['data_type'].split(" ")[0]

        if dtype in self.udt.keys():
            dtype = self.udt[dtype]['basetype']

        found = False
        for k, v in _DT_MAPPER.items():
            if dtype in v:
                found = True
                ret["type"] = _OPENAPI_FORMAT_MAPPER[k]
                ret["format"] = k

        if not found:
            ret["type"] = "string"
            ret["format"] = "oracle_" + dtype

        return ret