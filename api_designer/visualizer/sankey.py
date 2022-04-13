# **********copyright info*****************************************
# This code is copyright of EZAPI LLC. For further info, reach out to rams@ezapi.ai
# *****************************************************************


from api_designer import mongo
from api_designer.visualizer.element_scoring import enhance_attributes


def get_response_status(all_paths, path, method):
    res = []

    for p in all_paths:
        if p["endpoint"] == path and p["method"] == method:
            responseData = p["responseData"]

            for resp in responseData:
                res.append(resp["status_code"])

            break
    return res


def process_sankey_visualizer(projectid, db):  # tags can be multiple
    db.elements.remove({"projectid": projectid})
    db.sankey.remove({"projectid": projectid})

    project_data = db.projects.find_one({"projectId": projectid})
    project_type = project_data.get("projectType", None)

    if not project_data or not project_type:
        return {
            "success": False,
            "status": 404,
            "message": "project data or project type not found",
        }

    try:
        score_result, score_message = enhance_attributes(projectid, db, project_type)
        if score_result:
            all_elements = db.elements.find({"projectid": projectid})
            all_paths = db.operationdatas.find({"projectid": projectid})

            all_elements = list(all_elements)[0]
            all_paths = list(all_paths)
            all_paths = [x["data"] for x in all_paths]

            # todo - graph_data -> graph_metamodel
            graph_data = {}  # tag wise

            element_data = all_elements["data"]
            for elem in element_data:
                elem_tag = elem["tag"]
                elem_data = elem["data"]

                if elem_tag not in graph_data and elem_data:  # elem_data is not none
                    # NODES - name, apiops_type, tag
                    # LINKS - source, target, value
                    graph_data[elem_tag] = {"NODES": [], "LINKS": []}

                    # resource
                    resource = elem_tag.upper()
                    resource_name = resource + "|" + "tag" + "|" + elem_tag
                    resource_node = {
                        "name": resource_name,
                        "api_ops_type": "tag",
                        "tag": elem_tag,
                    }
                    graph_data[elem_tag]["NODES"].append(resource_node)

                    for elem, elem_spec in elem_data.items():

                        # element
                        elem_name = elem + "|" + "element" + "|" + elem_tag
                        elem_node = {
                            "name": elem_name,
                            "api_ops_type": "element",
                            "tag": elem_tag,
                        }
                        graph_data[elem_tag]["NODES"].append(elem_node)

                        # element -> resource
                        graph_data[elem_tag]["LINKS"].append(
                            {"source": elem_name, "target": resource_name, "value": 3}
                        )

                        for spec in elem_spec:
                            spec_summary = spec["summary"] or "summary"
                            spec_description = spec["description"] or "desc"
                            spec_method = spec["method"]
                            spec_path = spec["endpoint"]

                            # business function
                            bfunction_name = (
                                spec_description
                                + "|"
                                + "business-function"
                                + "|"
                                + elem_tag
                            )
                            bfunction_node = {
                                "name": bfunction_name,
                                "api_ops_type": "business-function",
                                "tag": elem_tag,
                            }
                            graph_data[elem_tag]["NODES"].append(bfunction_node)

                            # business function -> element
                            graph_data[elem_tag]["LINKS"].append(
                                {
                                    "source": bfunction_name,
                                    "target": elem_name,
                                    "value": 3,
                                }
                            )

                            # endpoint
                            endpoint_name = (
                                spec_path + "|" + "endpoint" + "|" + elem_tag
                            )
                            endpoint_node = {
                                "name": endpoint_name,
                                "api_ops_type": "endpoint",
                                "tag": elem_tag,
                            }  # "summary": spec_summary
                            graph_data[elem_tag]["NODES"].append(endpoint_node)

                            # resource -> endpoint
                            graph_data[elem_tag]["LINKS"].append(
                                {
                                    "source": resource_name,
                                    "target": endpoint_name,
                                    "value": 3,
                                }
                            )

                            # operation
                            operation_name = (
                                spec_description + "|" + "operation" + "|" + elem_tag
                            )
                            operation_node = {
                                "name": operation_name,
                                "api_ops_type": "operation",
                                "tag": elem_tag,
                            }
                            graph_data[elem_tag]["NODES"].append(operation_node)

                            # endpoint -> operation
                            graph_data[elem_tag]["LINKS"].append(
                                {
                                    "source": endpoint_name,
                                    "target": operation_name,
                                    "value": 3,
                                }
                            )

                            # status
                            all_status = get_response_status(
                                all_paths, spec_path, spec_method
                            )
                            for status in all_status:
                                # spec_path extra added as different endpoints can have same status code
                                status_name = (
                                    status
                                    + "|"
                                    + "status"
                                    + "|"
                                    + elem_tag
                                    + "|"
                                    + spec_description
                                )
                                status_node = {
                                    "name": status_name,
                                    "api_ops_type": "status",
                                    "tag": elem_tag,
                                }
                                graph_data[elem_tag]["NODES"].append(status_node)

                                # operation -> status
                                graph_data[elem_tag]["LINKS"].append(
                                    {
                                        "source": operation_name,
                                        "target": status_name,
                                        "value": 3,
                                    }
                                )

                    graph_data[elem_tag]["NODES"] = [
                        i
                        for n, i in enumerate(graph_data[elem_tag]["NODES"])
                        if i not in graph_data[elem_tag]["NODES"][:n]
                    ]
                    graph_data[elem_tag]["LINKS"] = [
                        i
                        for n, i in enumerate(graph_data[elem_tag]["LINKS"])
                        if i not in graph_data[elem_tag]["LINKS"][:n]
                    ]

            sankey_data = {}
            sankey_data["tags"] = list(graph_data.keys())
            sankey_data["graph"] = []

            for key, val in graph_data.items():
                sankey_data["graph"].append(
                    {"tag": key, "nodes": val["NODES"], "links": val["LINKS"]}
                )

            sankey_collection = "sankey"
            sankey_document = {
                "projectid": projectid,
                "api_ops_id": projectid,
                "data": sankey_data,
            }
            mongo.store_document(sankey_collection, sankey_document, db)

            res = {"status": 200, "success": True, "message": "ok"}
        else:
            res = {"status": 500, "success": False, "message": score_message}

    except Exception as e:
        print("Sankey Error - ", str(e))
        res = {
            "status": 500,
            "errorType": type(e).__name__,
            "error": str(e),
            "message": "Some error has occured in visualizing data",
            "success": False,
        }
    return res
