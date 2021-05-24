import csv

from api_designer import config
from api_designer.utils.common import *


def name_matching(name1, name2):  # snake case and wordlist
    direct_match_score = word_match(name1[0], name2[0])
    split_match_score = 0

    # elements with length <= 2
    null_elements = [x for x in name1[1] + name2[1] if len(x) <= 2]

    matches = []
    elements_taken_1 = []
    elements_taken_2 = []

    numerator = 0.0
    denominator = 0.0

    numerator_matched_length = 0
    denominator_length = sum([len(x) for x in name1[1] + name2[1]])

    for s1 in name1[1]:
        for s2 in name2[1]:
            match_score = word_match(s1, s2)
            if match_score > 0:
                matches.append((match_score, s1, s2))

    matches = sorted(matches, reverse=True)

    for m in matches:
        if m[1] not in elements_taken_1 and m[2] not in elements_taken_2:
            elements_taken_1.append(m[1])
            elements_taken_2.append(m[2])
            numerator += m[0] * 2

            numerator_matched_length += (len(m[1]) + len(m[2])) * m[0]

    denominator = len(name1[1]) + len(name2[1])
    # denominator -= min(len(null_elements), 1)

    if denominator > 0:
        split_match_score = min(numerator / denominator, 1)

        if split_match_score == 0.5:  # only one word matched
            split_match_score = numerator_matched_length / denominator_length

    return max([direct_match_score, split_match_score, 0])


def match_attributes(schema_name, schema_attributes, table_name, table_attributes):
    # Update attribute names with separate_prefix_suffix return values
    schema_attr_name = [x["name"] for x in schema_attributes]
    table_attr_name = [x["name"] for x in table_attributes]

    schema_attr_name = separate_prefix_suffix(schema_attr_name, remove=False)
    table_attr_name = separate_prefix_suffix(table_attr_name)

    for x in range(len(schema_attributes)):
        schema_attributes[x]["name2"] = schema_attr_name[x]

    for x in range(len(table_attributes)):
        table_attributes[x]["name2"] = table_attr_name[x]

    matches = []
    for sa in schema_attributes:
        for ta in table_attributes:
            sa_name = sa["name2"]
            ta_name = ta["name2"]

            sa_parent = "_".join(string_split(sa["parent"]))

            # sa_name2 = (schema_name + "_" + sa_name[0], [schema_name] + sa_name[1])
            sa_name2 = (sa_parent + "_" + sa_name[0], [sa_parent] + sa_name[1])

            score1 = name_matching(sa_name, ta_name)

            if score1 > 0:
                score2 = name_matching(sa_name2, ta_name)
            else:
                score2 = 0

            matches.append([max(score1, score2), sa["name"], ta["name"], sa["level"]])

    matches = sorted(matches, reverse=True)

    filtered_matches = []
    s_taken = []
    t_taken = []

    for m in matches:
        if m[0] >= 0.33 and m[1] not in s_taken and m[2] not in t_taken:
            filtered_matches.append(m)
            s_taken.append(m[1])
            t_taken.append(m[2])

    return filtered_matches


def solve_matching(schemas_data, table_data):
    combined_csv = open("match_combined.csv", "w")
    combined_csv_write = csv.writer(combined_csv)

    all_attribute_match = []

    schema_names = [x["name"] for x in schemas_data]
    table_names = [x["table"] for x in table_data]

    schema_names = separate_prefix_suffix(schema_names)
    table_names = separate_prefix_suffix(table_names)

    for x in range(len(schemas_data)):
        schemas_data[x]["name2"] = schema_names[x]

    for x in range(len(table_data)):
        table_data[x]["table2"] = table_names[x]

    matched_score = []
    matched_attributes = []

    for sd in schemas_data:
        for td in table_data:
            sd_name = sd["name2"]
            td_name = td["table2"]

            sd_attributes = sd["attributes"]
            td_attributes = td["attributes"]

            nm_score = name_matching(sd_name, td_name)
            attributes_score = match_attributes(
                sd_name[0], sd_attributes, td_name[0], td_attributes
            )  # match score, schem attr, table attr, schema ttr level

            total_as_score = 0.0
            tmp = []

            for p in attributes_score:
                total_as_score += p[0] * p[0]
                tmp.append([p[0], p[1], p[2], p[3]])
                all_attribute_match.append([sd["name"], td["table"]] + p)

            if total_as_score > 0:
                comb_score = 3 * nm_score + total_as_score
            else:
                comb_score = nm_score + total_as_score

            matched_score.append(
                [comb_score, nm_score, total_as_score, sd["name"], td["table"]]
            )
            matched_attributes.append([sd["name"], td["table"], tmp])

    matched_score = sorted(matched_score, reverse=True)

    attr_considered = []

    for m in matched_score:
        print(m)
        tmp = []

        attr_score = 0
        if m[0] >= 1:
            for aam in all_attribute_match:
                schema_attr_to_write = (
                    aam[0] + "_" + aam[3]
                )  # schema_name + "_" + attribute_name

                # Best Match
                if (
                    schema_attr_to_write not in attr_considered
                    and m[3] == aam[0]
                    and m[4] == aam[1]
                ):
                    attr_score += aam[2] * aam[2]  # attribute score
                    attr_considered.append(schema_attr_to_write)
                    # combined_csv_write.writerow(aam + m)

                    tmp.append(aam + ["best"])

                elif m[3] == aam[0] and m[4] == aam[1]:
                    tmp.append(aam + ["other"])

        for t in tmp:
            row = [m[3], m[4], m[1], attr_score] + t
            combined_csv_write.writerow(row)


def spec_ddl_matcher(projectid, db):
    schemas_data = db.schemas.find({"projectid": projectid})
    schemas_data = list(schemas_data)
    schemas_data = [s["data"] for s in schemas_data]

    tables_data = db.tables.find({"projectid": projectid})
    tables_data = list(tables_data)

    solve_matching(schemas_data, tables_data)
    return {"success": True, "message": "ok", "status": 200}
