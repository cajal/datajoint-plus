"""
Tools to validate user input.
"""

import re
from itertools import combinations

import numpy as np

from .errors import OverwriteError


def pairwise_disjoint_set_validation(sets:list, set_names:list=None, error=Exception):
    """
    Checks all pairs of sets in provided list for disjointness. Will raise error if any two sets are not disjoint.
    :param sets: list of sets to check.
    :param set_names: (optional) list of set names to index and provide in error message if disjoint check fails. \
        Length and order must match "sets". Defaults to generic error message.
    :param error: (optional) error to throw upon validation failure. Defaults to Exception.
    :returns: None if validation passes, error if fail. 
    """
    if set_names is not None:
        assert len(sets) == len(set_names), 'Length of sets must match length of set_names'

    set_combinations = list(combinations(np.arange(len(sets)), 2))
    for c in set_combinations:
        if not set.isdisjoint(sets[c[0]], sets[c[1]]):
            if set_names is not None:
                raise error(f'attributes in "{set_names[c[0]]}" and "{set_names[c[1]]}" must be disjoint.')
            else:
                raise error(f'attributes in at least two provided sets are not disjoint.')


def _validate_hash_name_type_and_parse_hash_len(hash_name, attributes):
    """
    Validates the attribute type of hash_name and extracts the character length of hash.

    :param hash_name: (str) hash_name to validate.
    :param attributes: (dict) dj_table.heading.attributes dictionary that hash_name will index into.

    :returns: 
        - error if validation fails
        - hash character length (int) if validation passes
    """
    try:
        hash_type = attributes[hash_name].type
    except KeyError:
        raise KeyError(f'hash_name "{hash_name}" not found in attributes.') from None

    _, m, e = hash_type.rpartition('varchar')
    assert m == 'varchar', 'hash_name attribute must be of varchar type'

    hash_len_match = re.findall('[0-9]+', e)
    assert hash_len_match and hash_len_match[0].isdigit(), 'hash_name attribute must contain a numeric value specifying hash character length.'
    
    hash_len = int(hash_len_match[0])
    assert hash_len > 0 and hash_len <= 32, 'hash character length must be within range: [1, 32].'

    return hash_len


def _is_overwrite_validated(attr, group, overwrite_rows):
    """
    Checks if attr is in group and is overwriteable.
    """
    assert isinstance(overwrite_rows, bool), 'overwrite_rows must be a boolean.'
    if not overwrite_rows:
        if attr in group:
            raise OverwriteError(f'Attribute "{attr}" already in rows. To overwrite, set overwrite_rows=True.')
        else:
            return True
    return True