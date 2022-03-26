from pprint import pprint

class DTDecoder:
    def __init__(self, props):
        self.props = props

    def decoder(self):
        ret = {}
        dtype = self.props['data_type'].lower()

        # -----------------------------------
        # Numeric Data Types (type - number) + MONEY
        NUMBER_TYPES = {
            "smallint": {"format": "int16", "minimum": (-1<<15), "maximum": (1<<15)-1},
            "integer": {"format": "int32", "minimum": (-1<<31), "maximum": (1<<31) -1},
            "bigint": {"format": "int64", "minimum": (-1<<63), "maximum": (1<<63)-1},
            "decimal": {"format": "double", "precision": self.props.get("numeric_scale")},
            "numeric": {"format": "double", "precision": self.props.get("numeric_scale")},
            "real": {"format": "float", "precision": 6},
            "double precision": {"format": "double", "precision": 15},
            "smallserial": {"format": "int16", "minimum": 1, "maximum": (1<<15)-1},
            "serial": {"format": "int32", "minimum": 1, "maximum": (1<<31)-1},
            "bigserial": {"format": "int64", "minimum": 1, "maximum": (1<<63)-1},
            "money": {"format": "currency", "precision": 2},
            "boolean": {"format": "boolean"}
        }
        
        if dtype in list(NUMBER_TYPES.keys()):
            ret["type"] = "number"
            ret = {**ret, **NUMBER_TYPES[dtype]}

        # -----------------------------------
        # Character Data Types (type - string)
        maxLength = self.props.get("character_maximum_length")
        STRING_TYPE = {
            "character varying": {"maxLength": maxLength},
            "varchar": {"maxLength": maxLength},
            "character": {"maxLength": maxLength or 1, "padding": True},
            "char": {"maxLength": maxLength or 1, "padding": True},
            "text": {}
        }

        if dtype in list(STRING_TYPE.keys()):
            ret["type"] = "string"
            ret = {**ret, **STRING_TYPE[dtype]}

        # -----------------------------------
        # Date/Time Types
        DT_TYPE = [
            "timestamp",    # both date and time
            "date",         # date only
            "time",         # time only
            "interval"
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

        # -----------------------------------
        # Enumerated Types
        DT_TYPE = ['ENUM']
        if dtype in DT_TYPE:
            ret["type"] = "string"
            ret["format"] = "enum"

        # -----------------------------------
        # Arrays Type
        DT_TYPE = ['array']
        if dtype in DT_TYPE:
            ret["type"] = "array"
            ret["format"] = self.props.get("udt_name")

        # -----------------------------------
        # Byte Type (binary string)
        DT_TYPE = ['bytea']
        if dtype in DT_TYPE:
            ret["type"] = "byte"

        return ret