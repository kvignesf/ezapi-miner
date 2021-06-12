# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************

import re
import sys
import string
import itertools
from random import Random


class Xeger(object):
    def __init__(self, limit=10, seed=None):
        super(Xeger, self).__init__()
        self._limit = limit
        self._cache = dict()

        self._random = Random()
        self.random_choice = self._random.choice
        self.random_int = self._random.randint
        if seed:
            self.seed(seed)

        self._alphabets = {
            "printable": string.printable,
            "letters": string.ascii_letters,
            "uppercase": string.ascii_uppercase,
            "lowercase": string.ascii_lowercase,
            "digits": string.digits,
            "punctuation": string.punctuation,
            "nondigits": string.ascii_letters + string.punctuation,
            "nonletters": string.digits + string.punctuation,
            "whitespace": string.whitespace,
            "nonwhitespace": string.printable.strip(),
            "normal": string.ascii_letters + string.digits + " ",
            "word": string.ascii_letters + string.digits + "_",
            "nonword": "".join(
                set(string.printable).difference(
                    string.ascii_letters + string.digits + "_"
                )
            ),
            "postalsafe": string.ascii_letters + string.digits + " .-#/",
            "urlsafe": string.ascii_letters + string.digits + "-._~",
            "domainsafe": string.ascii_letters + string.digits + "-",
        }

        self._categories = {
            "category_digit": lambda: self._alphabets["digits"],
            "category_not_digit": lambda: self._alphabets["nondigits"],
            "category_space": lambda: self._alphabets["whitespace"],
            "category_not_space": lambda: self._alphabets["nonwhitespace"],
            "category_word": lambda: self._alphabets["word"],
            "category_not_word": lambda: self._alphabets["nonword"],
        }

        self._cases = {
            "literal": lambda x: chr(x),
            "not_literal": lambda x: self.random_choice(
                string.printable.replace(chr(x), "")
            ),
            "at": lambda x: "",
            "in": lambda x: self._handle_in(x),
            "any": lambda x: self.random_choice(string.printable.replace("\n", "")),
            "range": lambda x: [chr(i) for i in range(x[0], x[1] + 1)],
            "category": lambda x: self._categories[str(x).lower()](),
            "branch": lambda x: "".join(
                self._handle_state(i) for i in self.random_choice(x[1])
            ),
            "subpattern": lambda x: self._handle_group(x),
            "assert": lambda x: "".join(self._handle_state(i) for i in x[1]),
            "assert_not": lambda x: "",
            "groupref": lambda x: self._cache[x],
            "min_repeat": lambda x: self._handle_repeat(*x),
            "max_repeat": lambda x: self._handle_repeat(*x),
            "negate": lambda x: [False],
        }

    def xeger(self, string_or_regex):
        try:
            pattern = string_or_regex.pattern
        except AttributeError:
            pattern = string_or_regex

        parsed = re.sre_parse.parse(pattern)
        result = self._build_string(parsed)
        self._cache.clear()
        return result

    @property
    def random(self):
        return self._random

    @random.setter
    def random(self, random_instance):
        self._random = random_instance
        self.random_choice = self._random.choice
        self.random_int = self._random.randint

    def seed(self, seed):
        self._random.seed(seed)

    def _build_string(self, parsed):
        newstr = []
        for state in parsed:
            newstr.append(self._handle_state(state))
        return "".join(newstr)

    def _handle_state(self, state):
        opcode, value = state
        return self._cases[str(opcode).lower()](value)

    def _handle_group(self, value):
        result = "".join(self._handle_state(i) for i in value[3])
        if value[0]:
            self._cache[value[0]] = result
        return result

    def _handle_in(self, value):
        candidates = list(itertools.chain(*(self._handle_state(i) for i in value)))
        if candidates[0] is False:
            candidates = set(string.printable).difference(candidates[1:])
            return self.random_choice(list(candidates))
        else:
            return self.random_choice(candidates)

    def _handle_repeat(self, start_range, end_range, value):
        result = []
        end_range = min((end_range, self._limit))
        times = self.random_int(start_range, max(start_range, end_range))
        for i in range(times):
            result.append("".join(self._handle_state(i) for i in value))
        return "".join(result)


def get_regex_string(patetrn):
    res = None

    try:
        x = Xeger()
        res = x.xeger(patetrn)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno, str(e))

    return res
