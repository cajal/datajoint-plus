"""
Tools to modify DataJoint table headings.
"""

import re

import numpy as np
import pandas as pd


def parse_definition(definition):
    """
    Parses DataJoint definitions. Extracts the following line types, where lines are separated by newlines `\\n`:
        - headers: a line that starts with hash `#`.
        - dependencies: a line that does not start with hash `#` and contains an arrow `->`.
        - attributes: a line that contains a colon `:`, and may contain a hash `#`, as long as the colon `:` precedes the hash `#`.
        - divider: a line that does not start with a hash `#` and contains a DataJoint divider `---`.
        - non-matches: a line that does not meet any of the above criteria. 
        
    :param definition: DataJoint table definition to be parsed.

    :returns: 
        parsed_inds (dict): dictionary of line types and matching line indexes. 
        parsed_contents (dict): a dictionary of line types and matching line contents.
        parsed_stats (dict): a dictionary of line types and a summary of the number of line matches for each type. 
    """

    lines = re.split('\n', definition) # seperate definition lines
    lines_without_spaces = [re.sub(' +', '', l) for l in lines]

    char_inds = []
    for l in lines_without_spaces:
        ind_dict = {}
        for name, char in zip(['hash', 'divider', 'arrow', 'colon'], ['#', '---', '->', ':']):
            ind_dict[name] = [m.start() for m in re.finditer(char, l)]
        char_inds.append(ind_dict)

    df = pd.DataFrame(char_inds)

    df['contains_hash'] = df.apply(lambda x: x.hash != [], axis=1)
    df['contains_divider'] = df.apply(lambda x: x.divider != [], axis=1)
    df['contains_arrow'] = df.apply(lambda x: x.arrow != [], axis=1)
    df['contains_colon'] = df.apply(lambda x: x.colon != [], axis=1)
    df['hash_pos_0'] = df.apply(lambda x: 0 in x.hash, axis=1)
    df['colon_before_hash'] = df.apply(lambda x: x.colon[0] < x.hash[0] if x.colon and x.hash else False, axis=1)

    header_query = "hash_pos_0 == True"
    dependency_query = "hash_pos_0 == False and contains_arrow == True"
    attribute_query = "(contains_colon==True and contains_hash==False) or colon_before_hash==True"
    divider_query = "hash_pos_0 == False and contains_divider == True"

    names = ['headers', 'dependencies', 'attributes', 'dividers']
    queries = [header_query, dependency_query, attribute_query, divider_query]

    parsed_inds = {}
    for name, query in zip(names, queries):
        q = df.query(query)
        df = df.drop(df.query(query).index.values)
        if len(q)> 0:
            parsed_inds[name] = q.index.values
        else:
            parsed_inds[name] = []
    parsed_inds['non-matches'] = df.index.values

    parsed_contents = {}
    parsed_stats = {}

    for n in names + ['non-matches']:
        parsed_contents[n] = [lines[i] for i in parsed_inds[n]]
        parsed_stats[n] = f'Found {len(parsed_inds[n])} line(s) matching the profile of {n}.'

    return parsed_inds, parsed_contents, parsed_stats


def reform_definition(parsed_inds, parsed_contents):
    """
    Reforms DataJoint definition after parsing by `parse_definition`. 

    :param parsed_inds (dict): see `parse_definition`
    :param parsed_contents (dict): see `parse_definition`

    :returns: DataJoint definition
    """
    n_lines = len(np.concatenate([i for i in parsed_inds.values()]))
    content_list = [''] * n_lines
    for ii, cc in zip(parsed_inds.values(), parsed_contents.values()):
        for i, c in zip(ii, cc):
            content_list[int(i)] = c

    definition = """"""
    for c in content_list:
        definition += c + """\n"""
    
    return definition
