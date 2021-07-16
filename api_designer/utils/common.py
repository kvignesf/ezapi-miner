# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************


import collections
import math
import json
import re
import time

from itertools import groupby
import wordninja

# import spacy
# nlp = spacy.load("en_core_web_sm")

match_dictionary = {"insured": "claimant", "sex": "gender", "relationship": "roles"}


def word_split(word):
    try:
        word = word.lower()
        res = wordninja.split(word)

        if not res:
            return [word]
        else:
            is_valid_split = True

            for x in res:
                if len(x) == 1 or not x.islower():
                    is_valid_split = False

            if is_valid_split:
                return res
            else:
                return [word]
    except:
        return [word]


# def get_root_words(wordlist):
#     try:
#         rootwords = []
#         tokens = nlp(" ".join(wordlist))
#         for tok in tokens:
#             rootwords.append(tok.lemma_)
#         return rootwords
#     except:
#         return wordlist


def average_list(lst):
    if len(lst) > 0:
        return sum(lst) / len(lst)
    return None


def is_camel_case(s):
    return s != s.lower() and s != s.upper() and "_" not in s


def is_snake_case(s):
    return "_" in s


def to_camel_case(snake_str):
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def camel_case_split(s):
    try:
        matches = re.finditer(
            ".+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)", s
        )
        res = [m.group(0) for m in matches]
        res = [x.lower() for x in res]
        return res
    except:
        return [s]


def special_character_split(s):
    res = re.split(r"[^a-zA-Z0-9]", s)
    res = [x.lower() for x in res]
    return res


def string_split_new(s):
    ret = None
    if is_camel_case(s):
        ret = camel_case_split(s)
    ret = special_character_split(s)

    # word splitting
    ret2 = []
    for r in ret:
        tmp = word_split(r)
        if tmp:
            ret2 += tmp
        else:
            ret2 += r

    # if len(ret) != len(ret2):
    #     print("  After splitter - ", s, ret, ret2)
    return ret2


def string_split(s):
    ret = None
    if is_camel_case(s):
        ret = camel_case_split(s)
    ret = special_character_split(s)

    return ret


# todo: print all abreviation matches and improve the logic
def check_abbreviation(word1, word2):
    abbr_score = 0

    if len(word1) < 3 or len(word2) < 3:
        return abbr_score

    if (
        not is_snake_case(word1)
        and not is_snake_case(word2)
        and not is_camel_case(word1)
        and not is_camel_case(word2)
    ):
        pattern = ".*".join(word1.lower())
        res1 = re.match("^" + pattern, word2.lower())

        pattern = ".*".join(word2.lower())
        res2 = re.match("^" + pattern, word1.lower())

        if res1 or res2:
            abbr_score += 0.6

            if word1[0] == word2[0] and word1[-1] == word2[-1]:
                abbr_score += 0.15

    return abbr_score

    # if check_abbreviation(word1, word2) and (len(word1) + len(word2) >= 10):
    #     return min(0.75, (total_len - 2) / total_len)


# changes required to convert one string to another
# Example1 - claim -> cleim (1 change, a -> e)
# Example2 - claim -> cliam (1 chane, transpose ai -> ia)
# Example3 - claim -> clim (1 change, insert a)


def osaDistance(s1, s2, transposition=True):
    s1 = s1.lower()
    s2 = s2.lower()

    if s1 == s2:
        return 0

    len1 = len(s1)
    len2 = len(s2)

    dp = []

    for i in range(len1 + 1):
        tmp = [0] * (len2 + 1)
        dp.append(tmp)

    for i in range(len1 + 1):
        dp[i][0] = i

    for j in range(len2 + 1):
        dp[0][j] = j

    for i in range(1, len1 + 1):
        for j in range(1, len2 + 1):
            if s1[i - 1] == s2[j - 1]:
                cost = 0
            else:
                cost = 1

            # deletion, insertion, substitution
            dp[i][j] = min(
                dp[i - 1][j] + 1, min(dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost)
            )

            if transposition:
                if (
                    i > 1
                    and j > 1
                    and (s1[i - 1] == s2[j - 2] and s1[i - 2] == s2[j - 1])
                ):
                    dp[i][j] = min(dp[i][j], dp[i - 2][j - 2] + cost)  # transposition

    return dp[len1][len2]


def word_match(word1, word2):
    word1 = word1.lower()
    word2 = word2.lower()

    total_len = len(word1 + word2)

    if word1 == word2:
        return 1

    if (word1 in match_dictionary and match_dictionary[word1] == word2) or (
        word2 in match_dictionary and match_dictionary[word2] == word1
    ):
        return 1

    if len(word1) > 4 and len(word2) > 4 and osaDistance(word1, word2) <= 1:
        return (total_len - 1) / total_len

    abbr_score = check_abbreviation(word1, word2)
    if abbr_score > 0:
        return abbr_score

    return 0


def all_equal(words):
    return words and words.count(words[0]) == len(words)


def get_common_word(items):
    common = None
    nw = len(items)

    if nw < 3:
        return None

    counter = {x: len(list(freq)) for x, freq in groupby(sorted(items))}

    for k, v in counter.items():
        if v >= 0.7 * nw and len(k) <= 3:  # prefix suffix max length = 3
            common = k
            break

    return common


"""
# Input - list of words
# Output - Joint string (snake-cased), (splitted word) list


# Example 1
--> Input
[
    claim-number
    clim-status
    operationId
]

-> output
[
    (claim_number, [claim, number]),
    (claim_status, [claim, status]),
    (operation_id, [operation, id]),
]


# Example 2
--> Input
[
    col_claim_number
    col_claim_status
    col_claim_detail
]

--> output
[
    (claim_number, [claim, number]),
    (claim_status, [claim, status]),
    (claim_detail, [claim, detail])
]
"""


def transform_naming(wordlist, remove=True):
    num_words = len(wordlist)

    prefix_suffix_exist = True
    prefix_list = []
    suffix_list = []

    ret = []

    for i in range(num_words):
        ss = string_split_new(wordlist[i])
        ret.append(ss)

        if len(ss) >= 2:
            prefix_list.append(ss[0])
            suffix_list.append(ss[-1])
        else:
            prefix_suffix_exist = False

    prefix = None
    suffix = None

    if prefix_suffix_exist:  # More than 70%
        prefix = get_common_word(prefix_list)
        suffix = get_common_word(suffix_list)

    if remove:
        for i, r in enumerate(ret):
            if prefix and suffix and r[0] == prefix and r[1] == suffix:
                tmp = r[1:-1]
            elif prefix and r[0] == prefix:
                tmp = r[1:]
            elif suffix and r[-1] == suffix:
                tmp = r[:-1]
            else:
                tmp = r

            ret[i] = ("_".join(tmp), tmp)

            # root_words = get_root_words(tmp)
            # root_words = tmp
            # transformed_wordlist = list(zip(tmp, root_words))
            # ret[i] = ("_".join(r), transformed_wordlist)
    else:
        for i, r in enumerate(ret):

            ret[i] = ("_".join(r), r)

            # root_words = get_root_words(r)
            # root_words = r
            # transformed_wordlist = list(zip(r, root_words))
            # ret[i] = ("_".join(r), transformed_wordlist)

    return ret


def separate_prefix_suffix(
    wordlist, remove=True
):  # returns snake-case string and wordlist
    num_words = len(wordlist)

    prefix_suffix_exist = True
    prefix_list = []
    suffix_list = []

    ret = []

    for i in range(num_words):
        ss = string_split_new(wordlist[i])
        ret.append(ss)

        if len(ss) >= 2:
            prefix_list.append(ss[0])
            suffix_list.append(ss[-1])
        else:
            prefix_suffix_exist = False

    prefix = None
    suffix = None

    if prefix_suffix_exist:  # More than 70%
        prefix = get_common_word(prefix_list)
        suffix = get_common_word(suffix_list)

    if remove:
        for i, r in enumerate(ret):
            if prefix and suffix and r[0] == prefix and r[1] == suffix:
                tmp = r[1:-1]
            elif prefix and r[0] == prefix:
                tmp = r[1:]
            elif suffix and r[-1] == suffix:
                tmp = r[:-1]
            else:
                tmp = r

            ret[i] = ("_".join(r), tmp)
    else:
        for i, r in enumerate(ret):
            ret[i] = ("_".join(r), r)

    return ret


def merge_dict(dict1, dict2):
    return {**dict1, **dict2}
