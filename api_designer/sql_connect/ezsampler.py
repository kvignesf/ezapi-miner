from collections import Counter
from numpy import isin
import random

class Sampler:
    def __init__(self, dtype, samples):
        self.dtype = dtype
        self.samples = [x for x in samples if x != None]    # None and 0 are different
        self.null = len(samples) - len(self.samples)
    
    def get_sample_data(self):
        ret = {}
        ret["samples"] = self.samples
        ret["null"] = self.null
        ret["repeat"] = None

        if not self.samples:
            return ret

        if isinstance(self.samples[0], str) or isinstance(self.samples[0], int):
            ret["total"] = len(self.samples)
            ret["unique"] = len(list(set(self.samples)))
            ret["repeat"] = round(ret["total"] / ret["unique"], 2)

        return ret