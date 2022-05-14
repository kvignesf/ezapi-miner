from collections import Counter
import random
import re


def is_spec(s):
    if not re.match(r"^[_\W]+$", s):
        return False
    else:
        return True


class PatternDetector:
    def __init__(self, samples):
        self.samples = samples

    def detector(self):
        self.length_array = [len(x) for x in self.samples if x]
        if self.length_array:
            self.min_length = min(self.length_array)
            self.max_length = max(self.length_array)

            self.char_dict = {}
            self.pos_dict = {}
            self.char_dist = []

            for s in self.samples:
                tmp = dict(Counter(s))
                self.char_dict = {**self.char_dict, **tmp}
                self.char_dist += list(tmp.keys())

                tmp2 = {x: y for x, y in enumerate(s)}
                for k, v in tmp2.items():
                    if k not in self.pos_dict:
                        self.pos_dict[k] = []
                    self.pos_dict[k].append(v)

            if self.min_length == self.max_length:
                self.method = "positional"
            else:
                self.method = "hybrid"

    def generate_positional(self, nlen):
        ret = ""
        for i in range(nlen):
            ret += random.choice(self.pos_dict[i])
        return ret

    def generate_hybrid(self, nlen):
        ret = ""
        for i in range(nlen):
            tmp = "".join(self.pos_dict[i])
            if len(self.pos_dict[i]) == 1:
                ret += self.pos_dict[i][0]
            elif tmp.isnumeric() or tmp.isalpha() or is_spec(tmp):
                ret += random.choice(self.pos_dict[i])
            else:
                ret += random.choice(self.char_dist)
        return ret

    def generate_data(self, n=1):
        ret = None
        self.detector()
        if not self.length_array:
            return ""
            
        nlen = random.randint(self.min_length, self.max_length)
        if self.method == "positional":
            ret = self.generate_positional(nlen)
        elif self.method == "hybrid":
            ret = self.generate_hybrid(nlen)
        return ret