"""General-purpose utilities"""

import inspect
import logging
import re
import sys

import pandas as pd
import requests
from datajoint import config
from datajoint.errors import _switch_adapted_types, _switch_filepath_types
from datajoint.table import QueryExpression
from datajoint.user_tables import UserTable

from .errors import ValidationError
from .version import __version__


class classproperty:
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


def split_full_table_name(full_table_name:str):
    """
    Splits full_table_name from DataJoint tables and returns a tuple of (database, table_name).

    :param (str): full_table_name from DataJoint tables
    
    :returns (tuple): (database, table_name)
    """
    return tuple(s.strip('`') for s in full_table_name.split('.'))


def reform_full_table_name(schema_name:str, table_name:str):
    """
    Reforms full_table_name from DataJoint schema name and a table_name.

    :param schema_name (str): name of schema
    :param table_name (str): name of table
    
    :returns: full_table_name
    """
    return '.'.join(['`'+schema_name+'`', '`'+table_name+'`'])


def format_table_name(table_name, snake_case=False, part=False):
    """
    Splits full_table_name from DataJoint tables and returns a tuple of (database, table_name).

    :param (str): full_table_name from DataJoint tables
    
    :returns (tuple): (database, table_name)
    """
    if not snake_case:
        if not part:
            return table_name.title().replace('_','').replace('#','')
        else:
            return table_name.title().replace('__','.').replace('_','').replace('#','')
    else:
        if not part:
            return table_name.lower().strip('_').replace('#','')
        else:
            return table_name.lower().replace('__','.').strip('_').replace('#','')


def format_rows_to_df(rows):
    """
    Formats rows as pandas dataframe.
    :param rows: pandas dataframe, datajoint query expression, dict or tuple
    :returns: pandas dataframe
    """
    if isinstance(rows, pd.DataFrame):
        rows = rows.copy()
    elif (inspect.isclass(rows) and issubclass(rows, QueryExpression)) or isinstance(rows, QueryExpression):
        rows = pd.DataFrame(rows.fetch())
    elif isinstance(rows, list) or isinstance(rows, tuple):
        rows = pd.DataFrame(rows)
    elif isinstance(rows, dict):
        rows = pd.DataFrame([rows])
    else:
        raise ValidationError('Format of rows not recognized. Try inserting a list of dictionaries, a DataJoint expression or a pandas dataframe.')

    return rows


def enable_datajoint_flags(enable_python_native_blobs=True):
    """
    Enable experimental datajoint features
    
    These flags are required by 0.12.0+ (for now).
    """
    config['enable_python_native_blobs'] = enable_python_native_blobs
    _switch_filepath_types(True)
    _switch_adapted_types(True)


def register_externals(external_stores):
    """
    Registers external stores to DataJoint.
    """
    if 'stores' not in config:
        config['stores'] = external_stores
    else:
        config['stores'].update(external_stores)


def make_store_dict(path):
    return {
        'protocol': 'file',
        'location': str(path),
        'stage': str(path)
    }


def _get_calling_context() -> locals:
    # get the calling namespace
    try:
        frame = inspect.currentframe().f_back
        context = frame.f_locals
    finally:
        del frame
    return context


def add_objects(objects, context=None):
    """
    Imports the adapters for a schema_name into the global namespace.
    """   
    if context is None:
        # if context is missing, use the calling namespace
        try:
            frame = inspect.currentframe().f_back
            context = frame.f_locals
        finally:
            del frame
    
    for name, obj in objects.items():
        context[name] = obj


def check_if_latest_version(source='github', return_latest=False):
    """
    Checks if imported DataJointPlus version matches latest from source. Logs warning if versions do not match.

    :param source: (str) Options:
        github
    :param return_latest: (bool) If True, returns the latest version
    """
    try:
        if source == 'github':
            _latest_version_text = re.search('__version__.*', requests.get(f"https://raw.githubusercontent.com/cajal/datajoint-plus/main/datajoint_plus/version.py").text).group()
            latest_version = _latest_version_text.split('=')[1].strip(' "'" '") if len(_latest_version_text.split('='))>1 else _latest_version_text.strip(' "'" '")
            if __version__ != latest_version:
                logging.warning(f'Imported datajoint_plus version, {__version__} does not match the latest version on Github, {latest_version}.')
        else:
            raise AttributeError('Source not recognized. "github" is the only supported source')

        if return_latest:
            return latest_version
    except:
        logging.warning(f'DataJointPlus version check failed.')


def goto(table_id):
    """
    Checks table_id's of DataJoint user classes in the current module and returns the class if a partial match to table_id is found. 
    
    :param: (str) table_id to check (found in user_class.table_id and in schema.tables)
    
    returns: class if a table_id match is found, otherwise None
    """
    for _, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj) and issubclass(obj, UserTable):
            if getattr(obj, 'table_id') in table_id:
                return obj
            else:
                # check parts
                for p in dir(obj):
                    if inspect.isclass(p) and issubclass(p, UserTable):
                        if getattr(p, 'table_id') in table_id:
                            return p
