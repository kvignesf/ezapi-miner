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
_DT_MAPPER = {
    "int32": ["tinyint", "smallint", "int"],
    "int64": ["bigint"],
    "float": ["smallmoney", "money", "float", "real"],
    "double": ["decimal", "numeric"],
    "byte": ["bit"],
    "binary": ["binary", "varbinary", "varbinary", "image"],
    "date": ["date"],
    "date-time": ["datetime", "datetime2", "smalldatetime", "datetimeoffset"],
}

class DataTypeMapper:
    def __init__(self, props, udt):
        self.props = props
        self.udt = udt

    def datatypedecoder(self, typeProp):
        ret = {"type": None, "format": None}
        dtype = typeProp.split(" ")[0]

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
            ret["format"] = "sql_server_" + dtype

        return ret

    def decoder(self):
        ret = {}
        dtype = self.props['datatype'].split(" ")[0]

        if dtype in self.udt.keys():
            dtype = self.udt[dtype]['basetype']

        # Numeric Data Types
        NUMBER_TYPES = {
            "bigint": {"format": "int64", "minimum": (-1<<63), "maximum": (1<<63)-1},
            "numeric": {"format": "double", "precision": self.props.get("SCALE")},
            "bit": {"format": "boolean"},
            "smallint": {"format": "int16", "minimum": (-1<<15), "maximum": (1<<15)-1},
            "decimal": {"format": "double", "precision": self.props.get("SCALE")},
            "smallmoney": {"format": "currency", "precision": self.props.get("SCALE")},
            "int": {"format": "int32", "minimum": (-1<<31), "maximum": (1<<31-1)},
            "tinyint": {"format": "int8", "minimum": 0, "maximum": (1<<8)-1},
            "money": {"format": "currency", "precision": self.props.get("SCALE")},
            "float": {"format": "float"},
            "real": {"format": "float"}
        }
        if dtype in list(NUMBER_TYPES.keys()):
            ret["type"] = "number"
            ret = {**ret, **NUMBER_TYPES[dtype]}


        # Date/Time Types
        DATETIME_TYPES = [
            "date",
            "datetimeoffset",
            "datetime2",
            "smalldatetime",
            "datetime",
            "time"
        ]
        for dt in DATETIME_TYPES:
            if dtype.startswith(dt):
                ret["type"] = "datetime"
                ret["format"] = dt

        # Character Data Types
        length = self.props.get("LENGTH")
        precision = self.props.get("PRECISION")
        STRING_TYPE = {
            "char": {"minLength": length, "maxLength": length},
            "varchar": {"maxLength": length},
            "text": {},
            "nchar": {"format": "unicode", "minLength": precision, "maxLength": precision},
            "nvarchar": {"format": "unicode", "maxLength": precision},
            "ntext": {"format": "unicode"},
            "binary": {"format": "binary", "minLength": length, "maxLength": length},
            "varbinary": {"format": "binary", "maxLength": length},
            # "image": {"format": "binary"}
        }
        if dtype in list(STRING_TYPE.keys()):
            ret["type"] = "string"
            ret = {**ret, **STRING_TYPE[dtype]}

        # Other Types
        OTHER_TYPES = [
            "sysname",
            "sql_variant",
            "uniqueidentifier",
            "xml",
            "cursor",
            "table",
            "rowversion",
            "hierarchy",
            "geometry",
            "geography",
            "image"
        ]
        for dt in OTHER_TYPES:
            if dtype.startswith(dt):
                ret["type"] = "mssql"
                ret["format"] = dt

        return ret