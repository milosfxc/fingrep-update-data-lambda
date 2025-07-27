from xml.etree import ElementTree as ET


def get_all_children(parent:str, path:str) -> dict:
    # Load the XBRL XML file
    tree = ET.parse(path)
    root = tree.getroot()

    # Define namespaces
    ns = {
        'link': 'http://www.xbrl.org/2003/linkbase',
        'xlink': 'http://www.w3.org/1999/xlink'
    }
    target_arc = 'presentationArc' if '-pre-' in path else 'calculationArc'
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
            sub_dict = get_all_children(parent=arc_to, path=path)
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

def read_calculation_schema(calculation_schema:str):
    tree = ET.fromstring(calculation_schema)
    ans = dict()
    all_keys = set()
    # Define namespaces
    ns = {
        'link': 'http://www.xbrl.org/2003/linkbase',
        'xlink': 'http://www.w3.org/1999/xlink'
    }
    calculation_links = tree.findall('.//link:calculationLink', ns)
    for calc_link in calculation_links:
        role = calc_link.attrib.get('{http://www.w3.org/1999/xlink}role')
        if 'balancesheet' in role.lower():
            print(role)
        elif 'statementsofoperation' in role.lower():
            print(role)
        elif 'cashflow' in role.lower():
            print(role)
        # Extract all calculationArc elements under this calculationLink
        calculation_arc = calc_link.findall('link:calculationArc', ns)
        for calc_arc in calculation_arc:
            calc_arc_from = calc_arc.attrib.get('{http://www.w3.org/1999/xlink}from').rsplit('_', 1)[0]
            calc_arc_to = calc_arc.attrib.get('{http://www.w3.org/1999/xlink}to').rsplit('_', 1)[0]
            print(f"from: {calc_arc_from} to: {calc_arc_to}")

