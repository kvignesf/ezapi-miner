import enum
from api_designer import config
from api_designer.utils.common import *

match_collection = "matcher"


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


def scale_score_values(all_documents):
    attribute_score = [t["attributes_match_score"] for t in all_documents]
    name_score = [t["name_match_score"] for t in all_documents]

    maxm_as = max(attribute_score)
    minm_as = min(attribute_score)
    maxm_ns = max(name_score)
    minm_ns = min(name_score)

    for index, doc in enumerate(all_documents):
        all_documents[index]["attributes_match_score"] = (
            doc["attributes_match_score"] - minm_as
        ) / (maxm_as - minm_as)
        all_documents[index]["name_match_score"] = (
            doc["name_match_score"] - minm_ns
        ) / (maxm_ns - minm_ns)
        all_documents[index]["final_score"] = (
            all_documents[index]["attributes_match_score"]
            + all_documents[index]["name_match_score"]
        ) / 2.0

    # Scale final_score b/w 0 and 1
    final_scores = [t["final_score"] for t in all_documents]
    minm_fs = min(final_scores)
    maxm_fs = max(final_scores)

    for index, doc in enumerate(all_documents):
        all_documents[index]["final_score"] = (doc["final_score"] - minm_fs) / (
            maxm_fs - minm_fs
        )

    return all_documents


def flag_score_thresholds(all_documents):
    full_attributes_taken = set()
    schemas_taken = set()

    for i, doc in enumerate(all_documents):
        for j, attr in enumerate(doc["attributes"]):
            if attr["match_score"] >= 0.8 and doc["final_score"] >= 0.8:
                attr_to_consider = doc["schema"] + "_" + attr["schema_attribute"]

                if attr_to_consider not in full_attributes_taken:
                    all_documents[i]["attributes"][j]["match_type"] = "Full"
                    full_attributes_taken.add(
                        doc["schema"] + "_" + attr["schema_attribute"]
                    )
                else:
                    all_documents[i]["attributes"][j]["match_type"] = "Partial"

                schemas_taken.add(doc["schema"])

            elif (attr["match_score"] >= 0.8 or doc["final_score"] >= 0.8) or (
                attr["match_level"] == 3
                and attr["match_score"] >= 0.66
                and doc["final_score"] >= 0.25
            ):
                all_documents[i]["attributes"][j]["match_type"] = "Partial"
                schemas_taken.add(doc["schema"])
            else:
                all_documents[i]["attributes"][j]["match_type"] = None

    for i, doc in enumerate(all_documents):
        if doc["final_score"] >= 0.8:
            all_documents[i]["match_type"] = "Full"
        elif doc["schema"] in schemas_taken:
            all_documents[i]["match_type"] = "Partial"
        else:
            all_documents[i]["match_type"] = None

    return all_documents


def solve_matching(schemas_data, table_data, projectid, db):
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

    best_attr_considered = []

    all_documents = []
    for m in matched_score:
        tmp = []

        match_document = {
            "projectid": projectid,
            "schema": m[3],
            "table": m[4],
            "name_match_score": m[1],
            "attributes_match_score": m[2],
            "final_score": m[0],
            "attributes": [],
        }

        if m[0] >= 1:
            for aam in all_attribute_match:
                if m[3] == aam[0] and m[4] == aam[1]:

                    tmp_attribute = {
                        "schema_attribute": aam[3],
                        "table_attribute": aam[4],
                        "match_score": aam[2],
                        "match_level": aam[5],
                        "match_type": None,
                    }

                    # # format - schema_table
                    # schema_attr_to_write = aam[0] + "_" + aam[3]

                    # # Best Match
                    # if schema_attr_to_write not in best_attr_considered:
                    #     best_attr_considered.append(schema_attr_to_write)
                    #     tmp_attribute["match_type"] = "best"

                    # else:
                    #     tmp_attribute["match_type"] = "other"

                    match_document["attributes"].append(tmp_attribute)

        all_documents.append(match_document)

    all_documents = scale_score_values(all_documents)
    all_documents = flag_score_thresholds(all_documents)

    config.store_bulk_document(match_collection, all_documents, db)

    # for doc in all_documents:
    #     config.store_document(match_collection, doc, db)

    """
    # ---------- CSV Generation Part ----------
    import csv

    combined_csv = open("match_combined.csv", "w")
    combined_csv_write = csv.writer(combined_csv)

    for doc in all_documents:
        row = [
            doc["schema"],
            doc["table"],
            doc["name_match_score"],
            doc["attributes_match_score"],
            doc["final_score"],
            doc["match_type"],
        ]

        for attr in doc["attributes"]:
            tmp = row + [
                attr["schema_attribute"],
                attr["table_attribute"],
                attr["match_score"],
                attr["match_level"],
                attr["match_type"],
            ]
            combined_csv_write.writerow(tmp)
    # ---------- CSV Part Ended ----------
    """


def spec_ddl_matcher(projectid, db):
    schemas_data = db.schemas.find({"projectid": projectid})
    schemas_data = list(schemas_data)
    schemas_data = [s["data"] for s in schemas_data]

    tables_data = db.tables.find({"projectid": projectid})
    tables_data = list(tables_data)

    solve_matching(schemas_data, tables_data, projectid, db)
    return {"success": True, "message": "ok", "status": 200}
