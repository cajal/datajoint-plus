"""General-purpose utilities"""

import inspect
import logging
import re
import sys
from unittest import mock

import numpy as np
import pandas as pd
import requests
from datajoint.errors import _switch_adapted_types, _switch_filepath_types
from datajoint.table import QueryExpression
from datajoint.user_tables import UserTable
from IPython.display import display
from ipywidgets.widgets import HBox, Label, Output

from .config import config
from .errors import OverwriteError, ValidationError
from .hash import generate_table_id
from .logging import getLogger
from .version import __version__

logger = getLogger(__name__)

class classproperty:
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


def wrap(item):
    if not isinstance(item, list) and not isinstance(item, tuple):
        item = [item]
    return item


def unwrap(item):
    if isinstance(item, list) or isinstance(item, tuple):
        if len(item) == 1:
            return item[0]
    return item


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
    elif isinstance(rows, np.ndarray) and (rows.dtype.fields is not None):
        rows = pd.DataFrame(rows)
    else:
        raise ValidationError('Format of rows not recognized. Try a list of dictionaries, a DataJoint expression, a DataJoint fetch object, or a pandas dataframe.')

    return rows


def load_dependencies(connection, force=False, verbose=True):
    """
    Loads dependencies in a DataJoint connection object.

    :param connection: (datajoint.connection) DataJoint connection object 
    :param force: (bool) default False. Whether to force reload.
    """
    if verbose:
        if force:
            output = Output()
            display(output)
            with output:
                pending_text = Label('Loading schema dependencies...')
                confirmation = Label('Success.')
                confirmation.layout.display = 'none'
                display(HBox([pending_text, confirmation]))
                connection.dependencies.load()
                confirmation.layout.display = None
    else:
        connection.dependencies.load(force=force)


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
                logger.warning(f'Imported datajoint_plus version, {__version__} does not match the latest version on Github, {latest_version}.')
        else:
            raise AttributeError('Source not recognized. "github" is the only supported source')

        if return_latest:
            return latest_version
    except:
        logger.warning(f'DataJointPlus version check failed.')


def goto(table_id=None, full_table_name=None, directory='__main__', warn=True):
    """
    Checks table_id's of DataJoint user classes in the current module and returns the class if a partial match to table_id or full_table_name is found. 
    
    :param table_id: (str) table_id to check (found in user_class.table_id and schema.tables)
    :param full_table_name: (str) full_table_name to check (found in user_class.full_table_name and schema.tables)
    :param directory: Options - 
        '__main__' (str) - default. searches calling module.
        directory - directory to search
    :param warn: (bool) If True, logs warnings
    returns: class if a table_id match is found, otherwise None
    """
    # handle table_id and full_table_name input
    assert ~((table_id is None) and (full_table_name is None)), 'Provide table_id or full_table_name'

    if full_table_name is not None:
        table_id_eval = generate_table_id(full_table_name)
        if table_id is not None:
            assert table_id == table_id_eval, "Provided table_id and table_id evaluated from full_table_name do not match."
        table_id = table_id_eval

    match = []
    def check_directory(d):
        for name, obj in inspect.getmembers(d):
            if name in ['key_source', '_master', 'master', 'UserTable']:
                continue
            if inspect.isclass(obj) and issubclass(obj, UserTable):
                try:
                    if table_id in obj.table_id:
                        match.append(obj)
                        return
                        
                    check_directory(obj)
                except:
                    if warn:
                        logger.warning(f'Could not check table_id for {name}')
                    continue
                
    
    if directory == '__main__':
        directory = sys.modules[directory]
    
    check_directory(directory)
    
    n_unique_matches = len(np.unique([m.table_id for m in match]))
    if n_unique_matches == 1:
        return match[0]
    elif n_unique_matches > 1:
        if warn:
            logger.warning(f'table_id matched to multiple tables.')
    elif n_unique_matches == 0:
        if warn:
            logger.warning(f'table_id did not match to any tables. Are you searching the correct directory?')


def user_choice_with_default_response(default_response=None):
    """Creates a replacement for the DataJoint `user_choice` function that will
    return a default response if one was provided."""

    def _user_choice(prompt, choices=("yes", "no"), default=None):
        """
        Prompts the user for confirmation.  The default value, if any, is capitalized.
        :param prompt: Information to display to the user.
        :param choices: an iterable of possible choices.
        :param default: default choice
        :param default_response: If default_response is provided, bypasses input and returns default_response
        :return: the user's choice
        """
        if default_response is not None:
            return default_response
        
        assert default is None or default in choices
        choice_list = ', '.join((choice.title() if choice == default else choice for choice in choices))
        response = None
        while response not in choices:
            response = input(prompt + ' [' + choice_list + ']: ')
            response = response.lower() if response else default
        return response

    return _user_choice


def default_user_choice(default:str):
    return mock.patch("datajoint.table.user_choice", new=user_choice_with_default_response(default))


class safedict(dict):
    err_msg = 'Cannot update safedict because overwrite = False'

    def __init__(self, warn=True, overwrite=False, logger=None, *args, **kwargs):
        """
        Extension of dict that can prevent update if key already present in dict.

        :param warn:  (bool) toggle warnings
        :param overwrite: (bool) 
            if False, dict update will be prevent if key found in dict. 
            if True, defaults to normal dict behavior
        :param logger: logger to use for warnings. defaults to generic logging. 
        """
        self.warn = warn
        self.overwrite = overwrite
        self.logger = logger if logger is not None else logging.getLogger('__main__')
        super().__init__(*args, **kwargs)
        
    def _key_in_dict(self, *args, **kwargs):
        in_dict = False
        for keys in [args, kwargs]:
            for key in keys:
                if isinstance(key, dict):
                    key = list(key.keys())[0]
                if key in self:
                    in_dict = True
                    if self.warn:
                        self.logger.warning(f'{key} already in safedict.')
        return in_dict
                    
    def update(self, *args, **kwargs):
        if self._key_in_dict(*args, **kwargs) and not self.overwrite:
            self.logger.error(self.err_msg)
            raise OverwriteError(self.err_msg)
        super().update(*args, **kwargs)
    
    def __setitem__(self, key, value):
        if self._key_in_dict(key) and not self.overwrite:
            self.logger.error(self.err_msg)
            raise OverwriteError(self.err_msg)
        super().__setitem__(key, value)
