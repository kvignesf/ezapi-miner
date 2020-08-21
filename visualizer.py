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


def fetch_sankey_data(apiopsid, tag):
    # apiopsid = 'cfe786bf3e764cb49747dd5a639e511a'

    db = db_manager.get_db_connection()

    all_elements = db.elements.find({'api_ops_id': apiopsid})
    all_responses = db.responses.find({'api_ops_id': apiopsid})

    all_elements = list(all_elements)
    all_responses = list(all_responses)

    pet_elements = all_elements[0][tag]
    # pet_elements = all_elements[0]['pet']

    NODES = []
    LINKS = []

    resource = tag.upper() + " Resource"
    NODES.append({"name": resource})

    for elem, elem_spec in pet_elements.items():   # key
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

            spec_desc_node = {'name': spec_desc}
            spec_method_node = {'name': spec_method}
            spec_path_node = {'name': spec_path}

            statuses = get_response_status(
                all_responses, spec_path, spec_method)
            for s in statuses:
                _node = {'name': s}
                _link = {'source': spec_method, 'target': s, 'value': 3.0}

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
                          'target': spec_method,
                          'value': 3.0})

    result = {
        "links": LINKS,
        "nodes": NODES
    }

    return result
