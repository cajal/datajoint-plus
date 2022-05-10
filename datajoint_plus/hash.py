import collections
import hashlib
import inspect

import pandas as pd
import simplejson
from datajoint.expression import QueryExpression

from .errors import ValidationError


def generate_hash(rows, add_constant_columns:dict=None):
    """
    Generates hash for provided rows. 

    :param rows (pd.DataFrame, dict): Rows to hash. `type(rows)` must be able to instantiate a pandas dataframe.
    :param add_constant_columns (dict):  Each key:value pair will be passed to the dataframe to be hashed as `df[k]=v`, adding a column length `len(df)` with name `k` and filled with values `v`. 

    :returns: md5 hash
    """
    df = pd.DataFrame(rows)
    if add_constant_columns is not None:
        assert isinstance(add_constant_columns, dict), f' arg add_constant_columns must be Python dictionary instance.'
        for k, v in add_constant_columns.items():
            df[k] = v
    # permutation invariant hashing
    df = df.sort_index(axis=1)
    df = df.sort_values(by=df.columns.tolist()) 
    encoded = simplejson.dumps(df.to_dict(orient='records')).encode()
    dhash = hashlib.md5()
    dhash.update(encoded)
    return dhash.hexdigest()


def _validate_rows_for_hashing(rows):
    """
    Validates rows for `generate_hash`.
    """
    validated = False
    if isinstance(rows, pd.DataFrame):
        pass
    elif (inspect.isclass(rows) and issubclass(rows, QueryExpression)) or isinstance(rows, QueryExpression):
        pass
    elif isinstance(rows, list) or isinstance(rows, tuple):
        for row in rows:
            assert isinstance(row, collections.abc.Mapping), 'Cannot hash attributes unless row attributes are named. Try inserting a pandas dataframe, a DataJoint expression or a list of dictionaries.'
        pass
    else:
        raise ValidationError('Format of rows not recognized. Try inserting a list of dictionaries, a DataJoint expression or a pandas dataframe.')

        
def validate_and_generate_hash(rows, **kwargs):
    """
    Generates hash for provided rows with row validation for DataJoint.

    :rows: see `generate_hash`
    :kwargs: passed to `generate_hash`

    :returns: hash from `generate_hash`
    """
    _validate_rows_for_hashing(rows)
    return generate_hash(rows, **kwargs)


def generate_table_id(full_table_name):
    """
    Generates table_id by hashing full_table_name.

    :param: full_table_name 
    :returns: table_id
    """
    return generate_hash([{'full_table_name': full_table_name}])