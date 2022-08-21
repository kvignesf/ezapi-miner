# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************


from api_designer import mongo
from api_designer.utils.common import *

from pprint import pprint
import time

match_collection = "matcher"

MIN_PARTIAL_SCHEMA_SCORE = 2
MIN_FULL_SCHEMA_SCORE = 3.5
MIN_PARTIAL_ATTR_SCORE = 0.5
MIN_FULL_ATTR_SCORE = 0.8


def transform_schema_data(schemas_data):
    schema_names = [x["name"] for x in schemas_data]
    schema_names = transform_naming(schema_names, remove=False)

    for x in range(len(schemas_data)):
        schemas_data[x]["name2"] = schema_names[x]
        schema_attributes = schemas_data[x]["attributes"]

        attr_names = [p["name"] for p in schema_attributes]
        attr_names = transform_naming(attr_names, remove=False)

        for y in range(len(schema_attributes)):
            schemas_data[x]["attributes"][y]["name2"] = attr_names[y]

            parent2 = (
                schemas_data[x]["attributes"][y]["parent"] or schemas_data[x]["name"]
            )
            parent2 = string_split_new(parent2)
            parent2 = "_".join(parent2)
            schemas_data[x]["attributes"][y]["parent2"] = parent2

    return schemas_data


def transform_tables_data(tables_data):
    #print("tables_data", tables_data, len(tables_data))
    table_names = [x["table"] for x in tables_data]
    table_names = transform_naming(table_names)
    #print("table_names", table_names)

    for x in range(len(tables_data)):
        tables_data[x]["name2"] = table_names[x]
        table_attributes = tables_data[x]["attributes"]
        #print("table_attributes", table_attributes, len(table_attributes))

        attr_names = [p["name"] for p in table_attributes if p is not None]
        attr_names = transform_naming(attr_names)

        #print("attr_names", attr_names)

        #print("len(table_attributes)..", len(table_attributes))

        for y in range(len(table_attributes)):
            #print("x", x)
            #print("y", y)
            tables_data[x]["attributes"][y]["name2"] = attr_names[y]

    return tables_data


"""
Example - 

name1 - (claim_number, [claim, number])
name2 - (claim_num, [claim, num])
"""


def name_matching(name1, name2):  # snake case and wordlist
    direct_match_score = word_match(name1[0], name2[0])
    split_match_score = 0

    matches = []

    taken1 = []
    taken2 = []

    numerator = 0.0
    numerator_length = 0

    denominator = len(name1[1] + name2[1])
    denominator_length = sum([len(x) for x in name1[1] + name2[1]])

    for s1 in name1[1]:
        for s2 in name2[1]:
            match_score = word_match(s1, s2)
            if match_score > 0:
                matches.append((match_score, s1, s2))

    matches = sorted(matches, reverse=False)
    for m in matches:
        if m[1] not in taken1 and m[2] not in taken2:
            taken1.append(m[1])
            taken2.append(m[2])

            numerator += m[0] * 2
            numerator_length += (len(m[1]) + len(m[2])) * m[0]

    if denominator > 0:
        split_match_score = min(numerator / denominator, 1)
        if split_match_score <= 0.5:
            split_match_score = numerator_length / denominator_length

    return max([direct_match_score, split_match_score, 0])


# todo - consider table name also as prefix
def match_attributes(schema_name, schema_attributes, table_name, table_attributes):
    matches = []

    for sa in schema_attributes:
        for ta in table_attributes:
            sa_name = sa["name2"]
            ta_name = ta["name2"]

            sa_parent = sa["parent2"]
            sa_name2 = (
                sa_parent + "_" + sa_name[0],
                sa_parent.split("_") + [x[0] for x in sa_name[1]],
            )

            score = name_matching(sa_name, ta_name)
            if score > 0:
                score = max(score, name_matching(sa_name2, ta_name))

            matches.append([score, sa["name"], ta["name"], sa["level"]])

    matches = sorted(matches, reverse=True)

    filtered_matches = []
    taken1 = []
    taken2 = []

    for m in matches:
        if m[0] >= 0.33 and m[1] not in taken1 and m[2] not in taken2:
            filtered_matches.append(m)
            taken1.append(m[1])
            taken2.append(m[2])

    return filtered_matches


def filter_matches(all_documents):
    full_attributes_taken = set()
    schemas_table_taken = set()

    for i, doc in enumerate(all_documents):
        for j, attr in enumerate(doc["attributes"]):
            attr_to_consider = doc["schema"] + "_" + attr["schema_attribute"]

            if (
                doc["final_score"] < MIN_PARTIAL_SCHEMA_SCORE
                or attr["match_score"] < MIN_PARTIAL_ATTR_SCORE
            ) and (attr["match_score"] != 1):
                all_documents[i]["attributes"][j]["match_type"] = None

            elif (
                doc["final_score"] >= MIN_FULL_SCHEMA_SCORE
                and attr["match_score"] >= MIN_FULL_ATTR_SCORE
                and attr_to_consider not in full_attributes_taken
            ):
                all_documents[i]["attributes"][j]["match_type"] = "Full"
                full_attributes_taken.add(attr_to_consider)
                schemas_table_taken.add(doc["schema"] + "_" + doc["table"])

            else:
                all_documents[i]["attributes"][j]["match_type"] = "Partial"
                schemas_table_taken.add(doc["schema"] + "_" + doc["table"])

    for i, doc in enumerate(all_documents):
        if doc["final_score"] >= MIN_FULL_SCHEMA_SCORE:
            all_documents[i]["match_type"] = "Full"
        elif (doc["schema"] + "_" + doc["table"]) in schemas_table_taken:
            all_documents[i]["match_type"] = "Partial"
        else:
            all_documents[i]["match_type"] = None

    return all_documents


def solve_matching(schemas_data, tables_data, projectid):
    print("Matching Started ", round(time.time(), 1))
    slen = [len(x["name2"][1]) for x in schemas_data]
    tlen = [len(x["name2"][1]) for x in tables_data]

    multiplier = average_list(tlen) / average_list(slen)
    multiplier = min(multiplier, 3)
    multiplier = max(multiplier, 1)

    matched_score = []
    all_macthed_attributes = []

    for sd in schemas_data:
        for td in tables_data:
            sd_name = sd["name2"]
            td_name = td["name2"]
            table_key = td["key"]

            sd_attributes = sd["attributes"]
            td_attributes = td["attributes"]

            # Filter only child attributes
            sd_attributes = [x for x in sd_attributes if x["is_child"]]  # changed

            nm_score = name_matching(sd_name, td_name)

            attribute_score = match_attributes(
                sd_name[0], sd_attributes, td_name[0], td_attributes
            )

            total_as_score = 0.0

            for p in attribute_score:
                total_as_score += p[0] * p[0]
                all_macthed_attributes.append([sd["name"], td["table"]] + p)

            if total_as_score > 0:
                comb_score = multiplier * nm_score + total_as_score
            else:
                comb_score = nm_score

            matched_score.append(
                [comb_score, nm_score, total_as_score, sd["name"], td["table"], table_key]
            )

        matched_score = sorted(matched_score, reverse=True)
        all_documents = []

        for m in matched_score:
            tmp = []

            match_document = {
                "projectid": projectid,
                "schema": m[3],
                "table": m[4],
                "key": m[5],
                "name_match_score": m[1],
                "attributes_match_score": m[2],
                "final_score": m[0],
                "match_type": None,
                "attributes": [],
            }

            if m[0] >= 1:
                for aam in all_macthed_attributes:
                    if m[3] == aam[0] and m[4] == aam[1]:
                        tmp_attribute = {
                            "schema_attribute": aam[3],
                            "table_attribute": aam[4],
                            "match_score": aam[2],
                            "match_level": aam[5],
                            "match_type": None,
                        }
                        match_document["attributes"].append(tmp_attribute)

            all_documents.append(match_document)

    all_documents = filter_matches(all_documents)
    all_documents = [x for x in all_documents if len(x["attributes"]) > 0]
    print("Matching Completed ", round(time.time(), 1))
    return all_documents


def spec_ddl_matcher(projectid, db):
    print("Request Receievd ", round(time.time(), 1))
    schemas_data = db.schemas.find({"projectid": projectid})
    print("Schemas Data Fetched ", round(time.time(), 1))
    schemas_data = list(schemas_data)
    schemas_data = [s["data"] for s in schemas_data]
    #print("Schemas data final", schemas_data)
    tables_data = db.tables.find({"projectid": projectid})
    print("Tables Data Fetched ", round(time.time(), 1))
    tables_data = list(tables_data)
    #print("Tables data final", tables_data)

    schemas_data = transform_schema_data(schemas_data)
    print("Schemas Data Transformed ", round(time.time(), 1))
    #print("schemas_data transformed final", schemas_data)
    tables_data = transform_tables_data(tables_data)
    print("Tables Data Transformed ", round(time.time(), 1))
    #print("Tables transformed final", tables_data)


    all_documents = solve_matching(schemas_data, tables_data, projectid)
    print("Inserting into DB ", round(time.time(), 1))
    mongo.store_bulk_document(match_collection, all_documents, db)

    """
    # ---------- CSV Generation Part ----------
    import csv

    csv_headers = [
        "Schema",
        "Table",
        "Name Score",
        "Attributes Score",
        "Final Score",
        "Match Type (Schema)",
        "Schema Attribute",
        "Table Attribute",
        "Match Score",
        "Match Level",
        "Match Type (Attribute)",
    ]

    combined_csv = open("match_combined_new.csv", "w")
    combined_csv_write = csv.writer(combined_csv)
    combined_csv_write.writerow(csv_headers)

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

    print("Response", round(time.time(), 1))
    return {"success": True, "message": "ok", "status": 200}
