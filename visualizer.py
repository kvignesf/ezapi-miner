from pprint import pprint

import pymongo
import db_manager


def get_response_status(all_responses, path, method):
    res = []
    for resp in all_responses:
        if resp["path"] == path and resp["method"] == method:
            params = resp["params"]
            for p in params:
                res.append(p["status_code"])
            break
    return res

# Format -
# {
# 	tags: []
# 	graph: [
# 		{'tag', 'nodes': [], 'links': []},
# 		{'tag', 'nodes': [], 'links': []},
# 		{'tag', 'nodes': [], 'links': []}
# 	]
# }


def fetch_sankey_data(apiopsid):   # tags can be multiple
    try:
        client, db = db_manager.get_db_connection()

        all_elements = db.elements.find({'api_ops_id': apiopsid})
        all_responses = db.responses.find({'api_ops_id': apiopsid})
        all_elements = list(all_elements)[0]
        all_responses = list(all_responses)

        # todo - graph_data -> graph_metamodel
        graph_data = {}  # tag wise

        element_data = all_elements['data']
        for elem in element_data:
            elem_tag = elem["tag"]
            elem_data = elem["data"]

            if elem_tag not in graph_data:
                # NODES - name, apiops_type, tag
                # LINKS - source, target, value
                graph_data[elem_tag] = {'NODES': [], 'LINKS': []}

                # resource
                resource = elem_tag.upper()
                resource_name = resource + "|" + "tag" + "|" + elem_tag
                resource_node = {"name": resource_name,
                                 "api_ops_type": "tag", "tag": elem_tag}
                graph_data[elem_tag]["NODES"].append(resource_node)

                for elem, elem_spec in elem_data.items():

                    # element
                    elem_name = elem + "|" + "element" + "|" + elem_tag
                    elem_node = {"name": elem_name,
                                 "api_ops_type": "element", "tag": elem_tag}
                    graph_data[elem_tag]["NODES"].append(elem_node)

                    # element -> resource
                    graph_data[elem_tag]["LINKS"].append(
                        {"source": elem_name, "target": resource_name, "value": 3})

                    for spec in elem_spec:
                        spec_summary = spec["summary"]
                        spec_description = spec["description"]
                        spec_method = spec["method"]
                        spec_path = spec["path"]

                        # business function
                        bfunction_name = spec_description + "|" + "business-function" + "|" + elem_tag
                        bfunction_node = {
                            "name": bfunction_name, "api_ops_type": "business-function", "tag": elem_tag}
                        graph_data[elem_tag]["NODES"].append(bfunction_node)

                        # business function -> element
                        graph_data[elem_tag]["LINKS"].append(
                            {"source": bfunction_name, "target": elem_name, "value": 3})

                        # endpoint
                        endpoint_name = spec_path + "|" + "endpoint" + "|" + elem_tag
                        endpoint_node = {"name": endpoint_name,
                                         "api_ops_type": "endpoint", "tag": elem_tag}   # "summary": spec_summary
                        graph_data[elem_tag]["NODES"].append(endpoint_node)

                        # resource -> endpoint
                        graph_data[elem_tag]["LINKS"].append(
                            {"source": resource_name, "target": endpoint_name, "value": 3})

                        # operation
                        operation_name = spec_description + "|" + "operation" + "|" + elem_tag
                        operation_node = {"name": operation_name,
                                          "api_ops_type": "operation", "tag": elem_tag}
                        graph_data[elem_tag]["NODES"].append(operation_node)

                        # endpoint -> operation
                        graph_data[elem_tag]["LINKS"].append(
                            {"source": endpoint_name, "target": operation_name, "value": 3})

                        # status
                        all_status = get_response_status(
                            all_responses, spec_path, spec_method)
                        for status in all_status:
                            # spec_path extra added as different endpoints can have same status code
                            status_name = status + "|" + "status" + "|" + elem_tag + "|" + spec_description
                            status_node = {"name": status_name,
                                           "api_ops_type": "status", "tag": elem_tag}
                            graph_data[elem_tag]["NODES"].append(status_node)

                            # operation -> status
                            graph_data[elem_tag]["LINKS"].append(
                                {"source": operation_name, "target": status_name, "value": 3})

                graph_data[elem_tag]["NODES"] = [i for n, i in enumerate(
                    graph_data[elem_tag]["NODES"]) if i not in graph_data[elem_tag]["NODES"][:n]]
                graph_data[elem_tag]["LINKS"] = [i for n, i in enumerate(
                    graph_data[elem_tag]["LINKS"]) if i not in graph_data[elem_tag]["LINKS"][:n]]

        sankey_data = {}
        sankey_data["tags"] = list(graph_data.keys())
        sankey_data["graph"] = []

        for key, val in graph_data.items():
            sankey_data["graph"].append({
                "tag": key,
                "nodes": val["NODES"],
                "links": val["LINKS"]
            })

        res = {
            'status': 200,
            'success': True,
            'message': 'ok',
            'data': sankey_data
        }

    except Exception as e:
        res = {
            'status': 500,
            'errorType': type(e).__name__,
            'message': str(e),
            'success': False
        }
    return res
