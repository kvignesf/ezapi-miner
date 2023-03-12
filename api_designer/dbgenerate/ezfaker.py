from faker import Faker
import pytz
import random
import re

from dateutil import parser


def generate_datetime(args, field, constraints, table_object, examples):
    examples = sorted(examples)
    F = Faker()

    min_date = None
    max_date = None

    if constraints and table_object:
        for c in constraints:
            if c["type"] == "both":
                lhs, rhs = c["lhs"], c["rhs"]
                cond = c["condition"]
                if rhs == field:
                    lhs, rhs = rhs, lhs

                if rhs not in table_object:
                    pass
                else:
                    if cond == ">=" or cond == ">":
                        if rhs in table_object:
                            min_date = table_object[rhs]
                    elif cond == "<=" or cond == "<":
                        if rhs in table_object:
                            max_date = table_object[rhs]
                    elif cond == "==":
                        if lhs in table_object:
                            min_date = table_object[lhs]
                        if rhs in table_object:
                            max_date = table_object[rhs]

    timezone = args.get("timezone")
    tz = None
    if timezone:
        tz = pytz.timezone("UTC")

    if min_date:
        try:
            min_date = parser.parse(min_date)
        except parser._parser.ParserError:
            min_date = None

    if max_date:
        try:
            max_date = parser.parse(max_date)
        except parser._parser.ParserError:
            max_date = None

    if not min_date:
        try:
            min_date = parser.parse(min(examples))
        except parser._parser.ParserError:
            min_date = None
    if not max_date:
        try:
            max_date = parser.parse(max(examples))
        except parser._parser.ParserError:
            max_date = None

    if min_date and max_date:
        ret = F.date_time_between(min_date, max_date, tz)
    elif min_date:
        ret = F.future_datetime(min_date, tz)
    elif max_date:
        ret = F.past_datetime(max_date, tz)
    else:
        ret = F.date_time(tz)

    if args.get("format") == "date":
        ret = ret.date()

    ret = ret.isoformat()
    ret = ret.split(".")[0]

    return ret


class EzFaker:
    def __init__(self, name, typedata):
        self.name = name
        self.type = typedata  # type, format
        self.fake = Faker()

    def setup_vocab(self):
        self.vocab = {
            "mobileNumber": {"value": "gen_phone()", "matchType": "word"},
            "name": {"value": "self.fake.name()", "matchType": "full"},
            "firstName": {"value": "self.fake.first_name()", "matchType": "word"},
            "lastName": {"value": "self.fake.last_name()", "matchType": "word"},
            "city": {"value": "self.fake.city()", "matchType": "full"},
            "countryCode": {"value": "self.fake.country_code()", "matchType": "word"},
            "email": {"value": "self.fake.profile()['mail']", "matchType": "full"},
            "phone": {"value": "self.fake.phone_number()", "matchType": "full"},
            "country": {"value": "self.fake.country()", "matchType": "full"},
            "emailAddress": {"value": "self.fake.profile()['mail']", "matchType": "word"},
            "socialSecurityNumber": {"value": "self.fake.ssn()", "matchType": "word"},
            "postalCode": {"value": "self.fake.postcode()", "matchType": "word"},
            "zipCode": {"value": "self.fake.postcode()", "matchType": "word"},
            "fullName": {"value": "self.fake.name()", "matchType": "word"},
            "cityName": {"value": "self.fake.city()", "matchType": "word"},
            "countryName": {"value": "self.fake.country()", "matchType": "word"},
            "phoneNumber": {"value": "self.fake.phone_number()", "matchType": "word"},
            "username": {"value": "self.fake.profile()['username']", "matchType": "full"},
        }

    # Reference - https://stackoverflow.com/a/26227853
    @staticmethod
    def gen_phone():  # mobile number
        first = str(random.randint(100, 999))
        second = str(random.randint(1, 888)).zfill(3)

        last = str(random.randint(1, 9998)).zfill(4)
        while last in ["1111", "2222", "3333", "4444", "5555", "6666", "7777", "8888"]:
            last = str(random.randint(1, 9998)).zfill(4)

        return "{}-{}-{}".format(first, second, last)

    @staticmethod
    def camel_case_words(identifier):
        matches = re.finditer(".+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)", identifier)
        res = [m.group(0) for m in matches]
        return res

    def faker_generator(self):
        res = None
        matched = False

        for k, v in self.vocab.items():
            if v["matchType"] == "full" and self.name.lower() == k.lower():
                matched = True

            if v["matchType"] == "word":
                words = EzFaker.camel_case_words(k)
                matched = all(w.lower() in self.name.lower() for w in words if w != None and self.name != None)

            if matched:
                try:
                    res = eval(v["value"])
                except Exception as e:
                    print(e)
                    res = None
                break
        return res
