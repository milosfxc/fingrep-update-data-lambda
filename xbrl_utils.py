from xml.etree import ElementTree as ET


def get_all_children(parent:str, path:str, target_arc:str) -> dict:
    # Load the XBRL XML file
    tree = ET.parse(path)
    root = tree.getroot()

    # Define namespaces
    ns = {
        'link': 'http://www.xbrl.org/2003/linkbase',
        'xlink': 'http://www.w3.org/1999/xlink'
    }
    arcs = root.findall(f".//link:{target_arc}", ns)
    ans = {}
    tags = []
    # Extract and print 'from' and 'to' attributes
    for arc in arcs:
        arc_from = arc.attrib.get(f"{{{ns['xlink']}}}from")
        arc_to = arc.attrib.get(f"{{{ns['xlink']}}}to")
        if arc_from == parent:
            if not arc_to.endswith('Abstract') and not arc_to.endswith('ExtensibleEnumeration'):
                tags.append(arc_to)
            sub_dict = get_all_children(parent=arc_to, path=path, target_arc=target_arc)
            if sub_dict:
                ans[arc_to] = sub_dict
    if tags:
        ans['tags'] = tags
    return ans


def get_all_children_as_list(xbrl_map: dict) -> list:
    ans = []
    for key, value in xbrl_map.items():
        if key == 'tags':
            ans.extend(value)
        else:
            ans.extend(get_all_children_as_list(value))
    return ans


