from api_designer import config

# from pprint import pprint
import collections
import math
import json
import re

# import spacy
import time

# sp = spacy.load("en_core_web_sm")


def is_camel_case(s):
    return s != s.lower() and s != s.upper() and "_" not in s


def is_under_score(s):
    return "_" in s


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
        not is_under_score(word1)
        and not is_under_score(word2)
        and not is_camel_case(word1)
        and not is_camel_case(word2)
    ):
        pattern = ".*".join(word1.lower())
        res1 = re.match("^" + pattern, word2.lower())

        pattern = ".*".join(word2.lower())
        res2 = re.match("^" + pattern, word1.lower())

        return res1 is not None or res2 is not None

    return False

    # else:
    #     # first 4 letters are same
    #     return word1[:4].lower() == word2[:4].lower()


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

    # todo - taking longer time to execute
    # w1 = sp(word1)
    # w2 = sp(word2)

    # if len(w1) == 1 and len(w2) == 1:
    #     if w1[0].lemma_ == w2[0].lemma_:
    #         return 1 - 2.0 / (len(word1) + len(word2))

    if check_abbreviation(word1, word2) and (len(word1) + len(word2) >= 10):
        return (total_len - 2) / total_len

    if len(word1) > 4 and len(word2) > 4 and osaDistance(word1, word2) <= 1:
        return (total_len - 2) / total_len

    return 0


def element_matching(word1, word2, elem1, elem2, weighted=True):  # elem2 has weights
    direct_match_score = None
    element_match_score = None

    # direct word match
    direct_match_score = word_match(word1, word2)

    # element match (after splitting)
    if weighted:
        elem_weight = {x[0]: x[1] for x in elem2}
        elem2 = [x[0] for x in elem2]

    matches = []
    for s1 in elem1:
        for s2 in elem2:
            match_score = word_match(s1, s2)
            if match_score > 0:
                matches.append((match_score, s1, s2))

    matched_elements = []
    matches = sorted(matches, reverse=True)

    numerator = 0.0
    denominator = 0.0

    for m in matches:
        if m[1] not in matched_elements:
            matched_elements.append(m[1])
            if weighted:
                numerator += m[0] * elem_weight[m[2]]
            else:
                numerator += m[0]

    if weighted:
        denominator = sum(elem_weight.values())
    else:
        denominator = max(len(elem1), len(elem2))  # average length

    if denominator > 0.0:
        element_match_score = numerator / denominator

    return max(direct_match_score or 0, element_match_score or 0)


def calculate_sentence_weight(sentence, vocab, num_sentences):  # idf
    st = string_split(sentence)
    st_dict = collections.Counter(st)
    tmp = []

    for k in st_dict:
        idf = round(math.log10(num_sentences / vocab[k]), 2)
        tmp.append((k, idf))

    return tmp


def calculate_vocabulary(sentences):
    vocab = []
    for st in sentences:
        vocab += string_split(st)

    vocab = collections.Counter(vocab)
    return vocab


def match_attributes(table_attributes, schema_attributes):
    matches = []

    for sa in schema_attributes:
        for ta in table_attributes:
            sa_name = sa["name"]
            ta_name = ta["name"]

            # Also check type, format for further matching
            split_sa = string_split(sa_name)
            split_ta = string_split(ta_name)
            score = element_matching(
                sa_name, ta_name, split_sa, split_ta, weighted=False
            )

            critical_score = 0.0
            if "required" in sa and sa["required"]:
                critical_score += 1.0
            if "key" in ta and ta["key"] in ["primary", "composite"]:
                critical_score += 1.0
            critical_score += score

            matches.append((score, sa_name, ta_name, critical_score))

    matches = sorted(matches, reverse=True)
    s_matches = []
    t_matches = []
    matches_filtered = []

    for m in matches:
        if m[0] >= 0.5 and m[1] not in s_matches and m[2] not in t_matches:
            matches_filtered.append(m)
            s_matches.append(m[1])
            t_matches.append(m[2])

    return matches_filtered


def solve_matching(schemas_data, table_data):
    n_schemas = len(schemas_data)
    n_tables = len(table_data)

    table_names = [t["table"] for t in table_data]
    db_vocab = calculate_vocabulary(table_names)

    matched_score = []
    matched_attributes = []

    for dt in table_data:
        dt_name = dt["table"]
        dt_attributes = dt["attributes"]
        dt_split = calculate_sentence_weight(dt_name, db_vocab, n_tables)

        for st in schemas_data:
            st_name = st["name"]
            st_attributes = st["attributes"]
            st_split = string_split(st_name)

            # table/schema name matching
            nm_score = element_matching(st_name, dt_name, st_split, dt_split)

            # attributes_matching
            attributes_score = match_attributes(dt_attributes, st_attributes)

            as_score = 0.0
            tmp = []
            for t in attributes_score:
                as_score += t[0] * t[0]
                tmp.append(
                    (t[0], t[1], t[2], t[3])
                )  # match score, schema attribute, table attribute, criticallity score

            matched_attributes.append([(st_name, dt_name), tmp])
            comb_score = 2 * nm_score + as_score
            matched_score.append((comb_score, nm_score, as_score, st_name, dt_name))

    matched_score = sorted(matched_score, reverse=True)
    filtered_matched_score = []
    schemas_considered = []

    for m in matched_score:
        if m[0] >= 3.5 and m[3] not in schemas_considered:
            schemas_considered.append(m[3])
            filtered_matched_score.append(m)

    schmea_table_mapping = dict()
    for ft in filtered_matched_score:
        schema_name = ft[3]
        table_name = ft[4]
        match_score = ft[0]
        name_match_score = ft[1]
        attribute_match_score = ft[2]

        schmea_table_mapping[schema_name] = {
            "table_name": table_name,
            "match_score": round(match_score, 2),
            "name_match_score": round(name_match_score, 2),
            "attribute_match_score": round(attribute_match_score, 2),
        }

    for ma in matched_attributes:
        st = ma[0][0]  # schema name
        dt = ma[0][1]  # table name
        attributes = ma[1]

        if st in schmea_table_mapping and schmea_table_mapping[st]["table_name"] == dt:
            schmea_table_mapping[st]["attributes"] = []

            for a in attributes:
                tmp = {
                    "schema_attr": a[1],
                    "table_attr": a[2],
                    "match_score": round(a[0], 2),
                    "critical_score": round(a[3], 2),
                }
                schmea_table_mapping[st]["attributes"].append(tmp)

    return schmea_table_mapping


def spec_ddl_matcher(api_design_id, db):
    schemas_data = db.schemas.find({"api_design_id": api_design_id})
    schemas_data = list(schemas_data)
    schemas_data = [s["data"] for s in schemas_data]

    tables_data = db.tables.find({"api_design_id": api_design_id})
    tables_data = list(tables_data)

    matched_data = solve_matching(schemas_data, tables_data)

    for k, v in matched_data.items():
        match_collection = "matcher"
        match_document = {"api_design_id": api_design_id, "schema_name": k, "data": v}

        config.store_document(match_collection, match_document, db)

    return {"success": True, "message": "ok", "status": 200}
