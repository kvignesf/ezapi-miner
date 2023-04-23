from pprint import pprint

class DTDecoder:
    def __init__(self, props):
        self.props = props

    def decoder(self):
        ret = {}
        dtype = self.props['data_type'].lower()

        # Numeric Data Types (type - number) + MONEY
        NUMBER_TYPES = {
            "integer": {"format": "int32", "minimum": (-1<<31), "maximum": (1<<31) -1},
            "number": {"format": "float", "precision": self.props.get("DATA_PRECISION"), "scale": self.props.get("DATA_SCALE")},
            "boolean": {"format": "boolean"},
            "float":{"format":"float"},
            "binary_float":{"format":"float"},
            "binay_double": {"format": "double"},
            "raw":{"format":"binary"},
            "blob":{"format":"binary"}
        }
        
        if dtype in list(NUMBER_TYPES.keys()):
            ret["type"] = "number"
            ret = {**ret, **NUMBER_TYPES[dtype]}

        # Character Data Types (type - string)
        length = self.props.get("data_length")
        precision = self.props.get("data_precision")
        STRING_TYPE = {
            "char": {"minLength": length, "maxLength": length},
            "varchar2": {"maxLength": length},
            "nchar": {"format": "unicode", "minLength": precision, "maxLength": precision},
            "nvarchar": {"format": "unicode", "maxLength": precision},
            "clob":{"maxLength":length},
            "nclob":{"maxLength":length},
            "boolean":{"format":"boolean"},
        }
        if dtype in list(STRING_TYPE.keys()):
            ret["type"] = "string"
            ret = {**ret, **STRING_TYPE[dtype]}


        # Date/Time Types
        DT_TYPE = [
            "timestamp",    # both date and time
            "date",         # date only   
            "interval year to month",
            "interval day to second"
            "timestamp with time zone",
            "timestamp with local time zone"
        ]
        
        matched = False
        for dt in DT_TYPE:
            if dtype.startswith(dt):
                ret["type"] = "datetime"    # Custom type
                ret["format"] = dt
                matched = True
                break
        
        if matched:
            ret["timezone"] = True if "with time zone" in dtype else False
            ret["precision"] = self.props.get("datetime_precision")


        # Other Types
        OTHER_TYPES = [
            "bfile",
            "enum",
            "array",
            "tsvector",
            "tsquery",
            "uuid",
            "xml",
            "json",
            "jsonb",
            "rowid",
            "urowid",
            "xmltype"
        ]

        # Geometric Type
        GEOMETRIC_TYPES = [
            "point",
            "line",
            "lseg",
            "box",
            "path",
            "polygon",
            "circle"
        ]

        # Network Address Type
        NETWORK_TYPES = [
            "inet",
            "cidr",
            "macaddr",
            "macaddr8"
        ]

        for dt in OTHER_TYPES + GEOMETRIC_TYPES + NETWORK_TYPES:
            if dtype.startswith(dt):
                ret["type"] = "oracle"
                ret["format"] = dt

        return ret