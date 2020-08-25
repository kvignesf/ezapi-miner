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


def fetch_sankey_data(apiopsid, tags):   # tags can be multiple
    db = db_manager.get_db_connection()

    all_elements = db.elements.find({'api_ops_id': apiopsid})
    all_responses = db.responses.find({'api_ops_id': apiopsid})
    all_elements = list(all_elements)[0]
    all_responses = list(all_responses)

    NODES = []
    LINKS = []

    if not tags:
        tags = list(all_elements.keys())
        tags.remove("_id")
        tags.remove("api_ops_id")

    for t in tags:
        if t in all_elements:

            tagged_elements = all_elements[t]

            for elem, elem_spec in tagged_elements.items():   # key
                resource = t.upper() + " Resource"
                NODES.append({"name": resource})

                elem_node = {'name': elem}
                if elem_node not in NODES:
                    NODES.append(elem_node)

                # element to root
                LINKS.append({'source': elem,
                              'target': resource,
                              'value': 3.0})

                for spec in elem_spec:
                    spec_desc = spec['description']
                    spec_method = spec['method']
                    spec_path = spec['path']

                    # For distinguishing purpose (de-clutter the visualization)
                    spec_method_2 = spec_desc + "|" + spec_method

                    spec_desc_node = {'name': spec_desc}
                    spec_method_node = {'name': spec_method_2}
                    spec_path_node = {'name': spec_path}

                    statuses = get_response_status(
                        all_responses, spec_path, spec_method)
                    for s in statuses:
                        # For distinguishing purpose (de-clutter the visualization)
                        tmp = s + "|" + spec_desc
                        _node = {'name': tmp}
                        _link = {'source': spec_method_2,
                                 'target': tmp, 'value': 3.0}

                        if _node not in NODES:
                            NODES.append(_node)
                        LINKS.append(_link)

                    if spec_desc_node not in NODES:
                        NODES.append(spec_desc_node)
                    if spec_method_node not in NODES:
                        NODES.append(spec_method_node)
                    if spec_path_node not in NODES:
                        NODES.append(spec_path_node)

                    # description to element
                    LINKS.append({'source': spec_desc,
                                  'target': elem,
                                  'value': 3.0})

                    # root to path
                    LINKS.append({'source': resource,
                                  'target': spec_path,
                                  'value': 3.0})

                    # path to method
                    LINKS.append({'source': spec_path,
                                  'target': spec_method_2,
                                  'value': 3.0})

    LINKS = [i for n, i in enumerate(LINKS) if i not in LINKS[:n]]
    NODES = [i for n, i in enumerate(NODES) if i not in NODES[:n]]

    result = {
        "links": LINKS,
        "nodes": NODES
    }

    pprint(result)
    return result
