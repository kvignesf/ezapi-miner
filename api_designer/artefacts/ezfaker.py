# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************


from faker import Faker
import random
import re
import string

fake = Faker()
Faker.seed(0)

from api_designer.artefacts.reverse_regex import get_regex_string

STRING_FILTERS = ["minLength", "maxLength", "pattern"]  # todo - pattern
NUMBER_FILTERS = ["minimum", "maximum"]
ARRAY_FILTERS = ["minItems", "maxItems"]
ENUM_FILTERS = ["enum", "default"]

# 32 bit signed integer
MIN_INT = 1  # -1 << 31 Update - Sept 14
MAX_INT = 1 << 31 - 1

# 64 bit signed integer
MIN_LONG = 1  # -1 << 63 Update - Sept 14
MAX_LONG = 1 << 63 - 1

# string
MIN_LEN_STRING = 6
MAX_LEN_STRING = 20

EZAPI_VOCAB = {
    "mobileNumber": {"value": "gen_phone()", "matchType": "word"},
    "firstName": {"value": "fake.first_name()", "matchType": "word"},
    "lastName": {"value": "fake.last_name()", "matchType": "word"},
    "city": {"value": "fake.city()", "matchType": "full"},
    "countryCode": {"value": "fake.country_code()", "matchType": "word"},
    "email": {"value": "fake.profile()['mail']", "matchType": "full"},
    "phone": {"value": "fake.phone_number()", "matchType": "full"},
    "country": {"value": "fake.country()", "matchType": "full"},
    "emailAddress": {"value": "fake.profile()['mail']", "matchType": "word"},
    "socialSecurityNumber": {"value": "fake.ssn()", "matchType": "word"},
    "postalCode": {"value": "fake.postcode()", "matchType": "word"},
    "zipCode": {"value": "fake.postcode()", "matchType": "word"},
    "fullName": {"value": "fake.name()", "matchType": "word"},
    "cityName": {"value": "fake.city()", "matchType": "word"},
    "countryName": {"value": "fake.country()", "matchType": "word"},
    "phoneNumber": {"value": "fake.phone_number()", "matchType": "word"},
    "username": {"value": "fake.profile()['username']", "matchType": "full"},
}

# Reference - https://stackoverflow.com/a/26227853
def gen_phone():  # mobile number
    first = str(random.randint(100, 999))
    second = str(random.randint(1, 888)).zfill(3)

    last = str(random.randint(1, 9998)).zfill(4)
    while last in ["1111", "2222", "3333", "4444", "5555", "6666", "7777", "8888"]:
        last = str(random.randint(1, 9998)).zfill(4)

    return "{}-{}-{}".format(first, second, last)


def camel_case_words(identifier):
    matches = re.finditer(
        ".+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)", identifier
    )
    res = [m.group(0) for m in matches]
    return res


def is_name_matched(key):
    if not key:
        return False

    res = None
    matched = False

    for k, v in EZAPI_VOCAB.items():
        if v["matchType"] == "full" and key.lower() == k.lower():
            matched = True

        if v["matchType"] == "word":
            words = camel_case_words(k)
            matched = all(w.lower() in key.lower() for w in words)

        if matched:
            try:
                res = eval(v["value"])
            except:
                res = None
            return res


def generate_field_data(param, param_key):
    ret = None
    param_type = param.get("type")
    param_format = param.get("format")
    param_enum = param.get("enum")
    param_default = param.get("default")
    param_example = param.get("example")

    if param_enum:
        ret = random.choice(param_enum)

    elif param_default:
        ret = param_default

    # integer - int32, long - int64, float, double
    elif param_type == "integer":
        if param_format == "int64":
            min_val = param.get("minimum", MIN_LONG)
            max_val = param.get("maximum", MAX_LONG)
        else:
            min_val = param.get("minimum", MIN_INT)
            max_val = param.get("maximum", MAX_INT)

        if param_example and isinstance(param_example, int):
            if param_example > 0:
                min_val = int(param_example * 0.9)
                max_val = int(param_example * 1.1)
            elif param_example < 0:
                min_val = int(param_example * 1.1)
                max_val = int(param_example * 0.9)

        ret = random.randint(min_val, max_val)

    elif param_type == "number" or param_format in ("long", "double"):
        min_val = param.get("minimum", MIN_LONG)
        max_val = param.get("maximum", MAX_LONG)

        if param_example and isinstance(param_example, int):
            if param_example > 0:
                min_val = int(param_example * 0.9)
                max_val = int(param_example * 1.1)
            elif param_example < 0:
                min_val = int(param_example * 1.1)
                max_val = int(param_example * 0.9)

        ret = round(random.uniform(min_val, max_val), 2)

    elif param_type == "boolean":
        ret = bool(random.getrandbits(1))

    elif param_type == "string":
        if param_format == "date":
            if "pattern" in param:
                param_pattern = param.get("pattern")
                ret = get_regex_string(param_pattern)
            else:
                ret = fake.date()
        elif param_format == "date-time":
            if "pattern" in param:
                param_pattern = param.get("pattern")
                ret = get_regex_string(param_pattern)
            else:
                ret = fake.iso8601()  # todo - tzinfo
        else:
            min_len = param.get("minLength", MIN_LEN_STRING)
            max_len = param.get("maxLength", MAX_LEN_STRING)
            ret = None

            vocab_result = is_name_matched(param_key)
            if vocab_result:
                ret = vocab_result

            if not ret:
                digits_only = False
                if "pattern" in param:
                    param_pattern = param.get("pattern")
                    ret = get_regex_string(param_pattern)

                if param_example and isinstance(param_example, str):
                    if param_example.isdigit():
                        digits_only = True
                    min_len = len(param_example)
                    max_len = len(param_example)

                random_len = random.randrange(min_len, max_len + 1)

                if not ret:
                    if digits_only:
                        ret = "".join(
                            random.choice(string.digits) for i in range(random_len)
                        )
                    else:
                        ret = "".join(
                            random.choice(string.ascii_lowercase)
                            for i in range(random_len)
                        )

    else:
        ret = "unidentified type"

    return ret
