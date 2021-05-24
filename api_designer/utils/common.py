import collections
import math
import json
import re
import time

from itertools import groupby

match_difctionary = {"insured": "claimant", "sex": "gender", "relationship": "roles"}


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


def string_split(s):
    if is_camel_case(s):
        return camel_case_split(s)
    return special_character_split(s)


def check_abbreviation(word1, word2):
    if len(word1) < 3 or len(word2) < 3:
        return False

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

        return res1 is not None or res2 is not None

    return False


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

    if (word1 in match_difctionary and match_difctionary[word1] == word2) or (
        word2 in match_difctionary and match_difctionary[word2] == word1
    ):
        return 1

    if len(word1) > 4 and len(word2) > 4 and osaDistance(word1, word2) <= 1:
        return (total_len - 1) / total_len

    if check_abbreviation(word1, word2) and (len(word1) + len(word2) >= 10):
        return min(0.75, (total_len - 2) / total_len)

    # Root word match
    # Similarity match

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


# todo: multiple prefix, suffix support
def separate_prefix_suffix(
    wordlist, remove=True
):  # returns snake-case string and wordlist
    num_words = len(wordlist)

    prefix_suffix_exist = True
    prefix_list = []
    suffix_list = []

    ret = []

    for i in range(num_words):
        ss = string_split(wordlist[i])
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
