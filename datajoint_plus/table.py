"""
Extensions of DataJoint Table
"""

from copy import copy
import logging

from datajoint.table import Table
from . import free_table, AndList
from datajoint_plus.hash import generate_table_id

from .version import __version__ as version


class Tables(Table):
    """
    Log of tables in each schema.
    Instances are callable.  Calls with table hash return the table entry. 
    Calls with hash and action update the table.
    """

    def __init__(self, arg, database=None):
        super().__init__()

        if isinstance(arg, Tables):
            # copy constructor
            self.database = arg.database
            self._connection = arg._connection
            self._definition = arg._definition
            self._user = arg._user
            return

        self.database = database
        self._connection = arg
        self._definition = f"""    # tables in `{database}`
        table_id       :varchar(32)  # unique hash of full_table_name
        ---
        full_table_name : varchar(450) # name of table 
        djp_version     : varchar(32)  # version of datajoint_plus used to generate table_id
        timestamp = CURRENT_TIMESTAMP : timestamp # timestamp of entry (not necessarily when table was created)
        """

        if not self.is_declared:
            self.declare()
            self.connection.dependencies.clear()
        self._user = self.connection.get_user()

    @property
    def definition(self):
        return self._definition

    @property
    def table_name(self):
        return '~tables'

    def __call__(self, table_id=None, full_table_name=None, action=None):
        """
        :param table_id: unique ID of table to insert
        :param full_table_name: full table name (database + table)
        :param action: Options - 
            add - inserts new table to log 
            delete - deletes table from log
        """
        try:
            if action == 'add':
                assert full_table_name is not None, 'full_table_name needed to add table'
                
                if table_id is None:
                    table_id = generate_table_id(full_table_name)

                self.insert1(
                    dict(
                        table_id=table_id, 
                        full_table_name=full_table_name, 
                        djp_version=version),
                        skip_duplicates=True, 
                        ignore_extra_fields=True
                    )   
      
            else:
                table_id_restr = f'table_id LIKE "{table_id}%"' if table_id is not None else None
                full_table_name_restr = {'full_table_name': full_table_name} if full_table_name is not None else None
                
                restr = [r for r in [table_id_restr, full_table_name_restr] if r is not None]
                restr = self & AndList(restr)

                if action == 'delete':
                    assert ~((table_id is None) and (full_table_name is None)), 'Provide table_id or full_table_name to delete.'
                    assert len(restr) == 1, 'There should be only one entry to delete.'
                    (self & restr).delete()

                if (table_id is None) and (full_table_name is None):
                    return self

                full_table_name = restr.fetch1('full_table_name')
                return free_table(self.connection, full_table_name)

        except Exception as e:
            logging.error(e)
            logging.info('failure interacting with ~tables')

    def delete(self):
        """bypass interactive prompts and cascading dependencies"""
        self.delete_quick()

    def drop(self):
        """bypass interactive prompts and cascading dependencies"""
        self.drop_quick()
