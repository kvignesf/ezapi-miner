from api_designer.dbgenerate.ezfaker import EzFaker, generate_datetime

from api_designer.dbgenerate.markov import Markov
from api_designer.dbgenerate.string_pattern_detection import PatternDetector

import random
import re


def get_coordinates(text, prefix):
    text = text.replace("\n", " ")
    text = re.sub("\t", " ", text)
    text = re.sub(" +", " ", text)
    text = text.strip()

    if prefix in text:
        start_index = text.find(prefix)
        text = text[start_index + len(prefix) :]
        text = text.strip()
        text = text.strip("(")
        text = text.strip(")")
        coords = text.split(",")
        X, Y = [], []

        for c in coords:
            c = c.strip(" ")
            c = c.split(" ")
            X.append(float(c[0]))
            Y.append(float(c[1]))
        return X, Y
    return None


def count_decimal_digits(x):
    x = str(x)
    x = x.split(".")
    if len(x) == 1:
        return 0
    x = x[1]
    if len(x) == 1 and x == "0":
        return 0
    else:
        return len(x)


class Generator:
    def __init__(self, args, constraints, table_object):
        self.args = args
        self.constraints = constraints
        self.table_objects = table_object  # Already generated data

        self.decoder = args.get("decoder", None)
        self.sample = args.get("sample", None)
        self.field = args["name"]
        self.type = self.decoder.get("type", None)
        self.format = self.decoder.get("format", None)

    def number_generator(self):
        ret = None
        if self.sample:
            examples = self.sample["samples"]
            null_count = self.sample["null"]
            value_count = len(examples)

            if null_count > 0 and value_count > 0:
                toss = random.random()
                if toss < null_count / (null_count + value_count):
                    return None

            if not examples:
                return None

            minval = min(examples)
            maxval = max(examples)
            if self.sample.get("repeat") and (
                self.sample["repeat"] >= 3 or (self.sample["repeat"] > 1 and self.sample["unique"] <= 10)
            ):
                ret = random.choice(examples)
            else:
                if "int" in self.decoder.get("format", ""):
                    ret = random.randint(minval, maxval)
                else:
                    precisions = [len(str(x).split(".")[1]) for x in examples]
                    min_precision = min(precisions)
                    max_precision = max(precisions)
                    ret = round(random.uniform(minval, maxval), random.randint(min_precision, max_precision))
        else:
            minval = self.decoder.get("minimum")
            maxval = self.decoder.get("maximum")
            if "int" in self.decoder.get("format", ""):
                ret = random.randint(minval, maxval)
            else:
                precision = self.decoder.get("preciison", 0)
                ret = round(random.uniform(minval, maxval), precision)
        return ret

    def datetime_generator(self):
        examples = self.sample.get("samples", None)
        ret = generate_datetime(self.decoder, self.field, self.constraints, self.table_objects, examples)
        return ret

    def string_generator(self):
        ret = self.faker_generator()

        if not ret and self.sample:
            examples = self.sample["samples"]
            examples = [x.strip() for x in examples]
            null_count = self.sample["null"]
            value_count = len(examples)

            if null_count > 0 and value_count > 0:
                toss = random.random()
                if toss < null_count / (null_count + value_count):
                    return None

            if not examples:
                return None

            if not self.sample["repeat"]:
                ret = random.choice(examples) or None
            elif self.sample["repeat"] >= 3 or (self.sample["repeat"] > 1 and self.sample["unique"] <= 10):
                ret = random.choice(examples)
            else:
                ex_len = [len(x.split(" ")) for x in examples]
                avglen = sum(ex_len) / len(examples)
                minlen = min(ex_len)
                maxlen = max(ex_len)
                if avglen > 1:
                    corpus = " ".join(examples)
                    ngram = 3 if avglen >= 3 else 2
                    M = Markov(corpus, ngram=ngram)
                    ret = M.generate_markov_text(random.randint(min(minlen, ngram), maxlen))

        if not ret:
            if examples:
                P = PatternDetector(examples)
                ret = P.generate_data()

        if self.decoder.get("maxLength"):
            maxlen = int(self.decoder.get("maxLength"))
            ret = ret[:maxlen]

        return ret

    def boolean_generator(self):
        return random.choice([True, False])

    def faker_generator(self):
        F = EzFaker(self.field, self.decoder)
        F.setup_vocab()
        return F.faker_generator()

    def mssql_generator(self):
        examples = self.sample["samples"]
        null_count = self.sample["null"]
        value_count = len(examples)

        if null_count > 0 and value_count > 0:
            toss = random.random()
            if toss < null_count / (null_count + value_count):
                return None

        if not examples:
            return None

        if self.format in ("geography", "geometry"):
            points = 0
            polygons = 0
            linstrings = 0

            Xs, Ys, Ns = [], [], []
            for ex in examples:
                ret = None
                if ex.startswith("POINT"):
                    points += 1
                    ret = get_coordinates(ex, "POINT")
                elif ex.startswith("POLYGON"):
                    polygons += 1
                    ret = get_coordinates(ex, "POLYGON")
                elif ex.startswith("LINESTRING"):
                    linstrings += 1
                    ret = get_coordinates(ex, "LINESTRING")

                if ret is not None:
                    Ns.append(len(ret[0]))
                    Xs += ret[0]
                    Ys += ret[1]

            minX, maxX = min(Xs), max(Xs)
            minY, maxY = min(Ys), max(Ys)
            round_digits = [count_decimal_digits(x) for x in Xs + Ys]

            n = random.choice(Ns)
            genX = []
            genY = []

            for _ in range(n):
                d = random.choice(round_digits)
                genX.append(round(random.uniform(minX, maxX), d))
                genY.append(round(random.uniform(minY, maxY), d))

            if n == 1:  # point
                ret = f"POINT({genX[0]} {genY[0]})"
            elif n == 2:  # linestring
                ret = "LINESTRING("
                for t in range(len(genX)):
                    ret += f"{genX[t]} {genY[t]}"
                    if t + 1 != len(genX):
                        ret += ", "
                ret += ")"
            elif n >= 4:  # polygon
                ret = "POLYGON(("
                for t in range(len(genX)):
                    ret += f"{genX[t]} {genY[t]}, "
                ret += f"{genX[0]} {genY[0]}"
                ret += "))"
            return ret
        elif self.format == "hierarchy":
            tmp = None
            while True:
                tmp = ""
                level = random.randint(1, 10)
                for _ in range(level):
                    tmp_int = random.randint(1, 10)
                    tmp += "/" + str(tmp_int)
                tmp += "/"

                if tmp not in examples:
                    break
            return tmp
        elif self.args.get("datatype") == "image":
            # image = open("./hello.png", "rb")
            # image = image.read()
            # ret = f"cast({image} as varbinary(max)))"
            try:
                txt = open("./hex_image.txt")
                txt = txt.read()
                return txt
            except Exception as e:
                return "0x1234"
        else:
            return random.choice(examples)

    def postgres_generator(self):
        examples = self.sample["samples"]
        null_count = self.sample["null"]
        value_count = len(examples)

        if null_count > 0 and value_count > 0:
            toss = random.random()
            if toss < null_count / (null_count + value_count):
                return None

        if not examples:
            return None

        if self.format == "array":
            ret = random.choice(examples)
            ret = str(ret)
            ret = ret.replace("[", "{")
            ret = ret.replace("]", "}")

        else:
            ret = random.choice(examples)
        if self.format == 'jsonb':
            ret = str(ret)
            ret = ret.replace("'", '"')

        return ret

    def oracle_generator(self):
        examples = self.sample["samples"]
        null_count = self.sample["null"]
        value_count = len(examples)

        if null_count > 0 and value_count > 0:
            toss = random.random()
            if toss < null_count / (null_count + value_count):
                return None

        if not examples:
            return None

        if self.format == "array":
            ret = random.choice(examples)
            ret = str(ret)
            ret = ret.replace("[", "{")
            ret = ret.replace("]", "}")

        else:
            ret = random.choice(examples)
        if self.format == 'jsonb':
            ret = str(ret)
            ret = ret.replace("'", '"')

        return ret

    def generate_data(self, n=1):
        ret = None

        if self.type == "string":
            ret = self.string_generator()
        elif self.type == "boolean":
            ret = self.boolean_generator()
        elif self.type == "number":
            ret = self.number_generator()
        elif self.type == "datetime":
            ret = self.datetime_generator()
        elif self.type == "mssql":
            ret = self.mssql_generator()
        elif self.type == "postgres":
            ret = self.postgres_generator()
        elif self.type == "oracle":
            ret = self.oracle_generator()
        else:
            print(f"** Error - Unable to generate data for {self.field} with data type {self.type}")

        return ret
