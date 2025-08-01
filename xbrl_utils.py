import re
from typing import Optional
from xml.etree import ElementTree as ET

import edgar
from numpy.ma.core import max_val


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

def find_position_grouping_xbrl_tag(calculation_schema:str, statement_role:str, xbrl_tags:set[str]) -> Optional[set[str]]:
    """
    The function checks calculation arcs to find positions that were created by aggregating multiple other positions, in order to exclude them and prevent double counting.
    :param calculation_schema: A file from edgar filing that ends with _cal.xml
    :param statement_role: Use `find_calculation_schema_statement_role()` to get statement role
    :param xbrl_tags: A set of xbrl tags for balance sheet, income statement and cash flow statement positions.
    :return: A set of positions safe to sum without double counting.
    """
    tree = ET.fromstring(calculation_schema)
    hierarchy_map = {}
    # Define namespaces
    ns = {
        'link': 'http://www.xbrl.org/2003/linkbase',
        'xlink': 'http://www.w3.org/1999/xlink'
    }
    calculation_links = tree.findall('.//link:calculationLink', ns)
    for calculation_link in calculation_links:
        role = calculation_link.attrib.get('{http://www.w3.org/1999/xlink}role')
        if statement_role == role:
            # Extract all calculationArc elements under this calculationLink
            calculation_arcs = calculation_link.findall('link:calculationArc', ns)
            for cal_arc in calculation_arcs:
                calc_arc_from = get_xbrl_tag_from_calculation_arc(cal_arc.attrib.get('{http://www.w3.org/1999/xlink}from'))
                calc_arc_to = get_xbrl_tag_from_calculation_arc(cal_arc.attrib.get('{http://www.w3.org/1999/xlink}to'))
                if not calc_arc_from or not calc_arc_to: continue
                print(calc_arc_from)
                if calc_arc_from in xbrl_tags:
                    if calc_arc_from in hierarchy_map:
                        hierarchy_map[calc_arc_from].append(calc_arc_to)
                    else:
                        hierarchy_map[calc_arc_from] = [calc_arc_to]
        # Find grouping tag
        if hierarchy_map:
            all_values = {item for sublist in hierarchy_map.values() for item in sublist}
            grouping_tags = set(hierarchy_map.keys()).difference(all_values)
            return grouping_tags

        break
    return None


def find_calculation_schema_statement_role(calculation_schema:str, xbrl_tags:set[str]) -> Optional[str]:
    """
    Function returns a role/calculation link that has the most xbrl_tags. Roles must match more than 5 xbrl tags in order to be considered.
    :param calculation_schema: A file from edgar filing that ends with _cal.xml
    :param xbrl_tags: A set of xbrl tags for balance sheet, income statement and cash flow statement.
    :return: Role/CalculationLink that has more than 5 tags matched.
    """
    tree = ET.fromstring(calculation_schema)
    counter_map = {}
    # Define namespaces
    ns = {
        'link': 'http://www.xbrl.org/2003/linkbase',
        'xlink': 'http://www.w3.org/1999/xlink'
    }
    calculation_links = tree.findall('.//link:calculationLink', ns)
    for calculation_link in calculation_links:
        role = calculation_link.attrib.get('{http://www.w3.org/1999/xlink}role')
        counter_map[role] = 0
        xbrl_tags_temp = xbrl_tags
        if 'balancesheet' in role.lower():
            counter = 0
            # Extract all calculationArc elements under this calculationLink
            calculation_arcs = calculation_link.findall('link:calculationArc', ns)
            for cal_arc in calculation_arcs:
                calc_arc_from = get_xbrl_tag_from_calculation_arc(cal_arc.attrib.get('{http://www.w3.org/1999/xlink}from'))
                if calc_arc_from in xbrl_tags_temp:
                    xbrl_tags_temp.remove(calc_arc_from)
                    counter+= 1
                calc_arc_to = get_xbrl_tag_from_calculation_arc(cal_arc.attrib.get('{http://www.w3.org/1999/xlink}to'))
                if calc_arc_to in xbrl_tags_temp:
                    xbrl_tags_temp.remove(calc_arc_to)
                    counter += 1
            # Add number of tags that persist in the calculationLink
            if counter > 5: # No point to add roles with less than 5 tags as max function can return role with 0 matches
                counter_map[role] = counter
    return max(counter_map, key=counter_map.get, default=None)


def get_xbrl_tag_from_calculation_arc(xlink_str:str) -> Optional[str]:
    """
    Extracts us-gaap or ifrs-full tags from xlink:to and xlink:from locators.
    :param xlink_str: xlink:from or xlink:to attribute value.
    :return: The cleaned XBRL tag (e.g., "ifrs-full:Liabilities"), or `None` if no match is found.
    """
    if not xlink_str: return None
    match = re.search(r'(us|ifrs)[\W_]?(gaap|full)(?![\s])[\W_]?([a-zA-Z0-9]+)', xlink_str, re.IGNORECASE)
    if match:
        acc_standard = f"{match.group(1)}-{match.group(2)}".lower()
        position = match.group(3)
        return f"{acc_standard}:{position}"
    return None


def find_filling_attachment(filing:edgar.Filing, attachment_name_end:str) -> str|None:
    """
    The function searches for filing documents such as calculations, definitions, presentations, etc.
    :param filing: The edgar_tools filing object
    :param attachment_name_end: The last few characters of filing documents
    :return: Attachment in string format or None
    """
    attachments = filing.attachments
    for attach in attachments:
        if attach.document.endswith(attachment_name_end):
            return attach.text()
    return None
