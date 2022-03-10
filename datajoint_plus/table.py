"""
Extensions of DataJoint Table
"""

import logging

import datajoint as dj
from datajoint_plus.hash import generate_table_id
from .utils import goto, wrap
from .version import __version__ as version

class Table(dj.table.Table):
    """
    Extensions to DataJoint Table
    """
    
    _tables_ = None

    def declare(self, context=None):
        super().declare(context=context)
        self._tables(self.table_id, self.full_table_name, action='add')
    
    def drop(self):
        dj.table.Table.drop(self)
        self._tables(self.table_id, action='delete')

    def drop_quick(self):
        dj.table.Table.drop_quick(self)
        self._tables(self.table_id, action='delete')

    def goto(self, table_id=None, full_table_name=None, tid_attr=None, ftn_attr=None, directory='__main__', return_free_table=False, ftn_lookup=None):
        """
        Extends datajoint_plus.utils.goto with the additional options:
            :param tid_attr: (str) default=None, attribute in self that points to a table_id. Will be used as such: table_id = self.fetch1(tid_attr)
            :param ftn_attr: (str) default=None, attribute in self that points to a full_table_name. Will be used as such: full_table_name = self.fetch1(ftn_attr)
            :param return_free_table: (bool) Provides a FreeTable generated from full_table_name instead of using goto. 
                If only table_id is known, ftn_lookup should provide a mapping between table_id and full_table_name. 
            :param ftn_lookup: (DJ table) DataJoint table to query table_id and retrieve full_table_name if return_free_table is True
                By default the following query will be attempted: (ftn_lookup & {'table_id': table_id}).fetch1('full_table_name')
                If ftn_lookup uses a different attr name for table_id, provide it to tid_attr.
        """
        if isinstance(self, str):
            raise AttributeError('Instantiate table to run goto().')
        
        if len(self)==1:
            try:
                table_id = self.fetch1('table_id')
            except:
                pass
        
        if table_id is None and tid_attr is not None:
            table_id = self.fetch1(tid_attr)
        
        if full_table_name is None and ftn_attr is not None:
            full_table_name = self.fetch1(ftn_attr)

        if full_table_name is None and table_id is None:
            raise Exception('Provide either table_id/full_table_name or tid_attr/ ftn_attr.')

        if return_free_table:
            if full_table_name is None:
                try:
                    if tid_attr is None:
                        tid_attr = 'table_id'
                    full_table_name = (ftn_lookup & {tid_attr: table_id}).fetch1('full_table_name')
                except Exception as e:
                    raise Exception('If return_free_table=True and full_table_name is not provided, table_id and ftn_lookup must be provided.')
            return FreeTable(self.connection, full_table_name)
        
        else:
            return goto(table_id=table_id, full_table_name=full_table_name, directory=directory)

    def get(self, key=None, attrs=None):
        """
        Returns a single entry from table as a dictionary. 

        Table must be restricted to a single entry before calling get(), or key to restrict table can be passed to get(). 
        Optionally, attrs can be passed to return a subset of table attributes. 

        :param key: (dict, QueryExpression, AndList, etc) a restriction for table
        :param attrs: (str or list/ tuple) A single attr can be provided as a str, or a list/ tuple of strings

        :returns: (dict) Dictionary containing fetch1 results
        """
        try:
            if attrs is not None:
                attrs = wrap(attrs)

            if key is None:
                if attrs is not None:
                    result = self.fetch1(*attrs)
                    result = wrap(result)
                    return {a: r for a, r in zip(attrs, result)} # always return as dict
                else:
                    return self.fetch1()
            else:
                if attrs is not None:
                    result = (self & key).fetch1(*attrs)
                    result = wrap(result)
                    return {a: r for a, r in zip(attrs, result)}
                else:
                    return (self & key).fetch1()
        except AttributeError as e:
            raise AttributeError(e.args[0] + f'. Did you instantiate the class?') from None

    @property
    def _tables(self):
        if self._tables_ is None:
            self._tables_ = Tables(self.connection, database=self.database)
        return self._tables_


class FreeTable(Table, dj.FreeTable):
    """
    Extension to DataJoint FreeTable
    """
    
    def drop(self):
        dj.table.Table.drop(self)
        self._tables(full_table_name = self.full_table_name, action='delete')

    def drop_quick(self):
        dj.table.Table.drop_quick(self)
        self._tables(full_table_name = self.full_table_name, action='delete')


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
        table_id        :varchar(32)  # unique hash of full_table_name
        ---
        full_table_name : varchar(450) # name of table
        exists          : tinyint  # 1 - table exists in schema; 0 - table no longer exists
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

    def __call__(self, table_id=None, full_table_name=None, action=None, directory='__main__'):
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
                else:
                    assert table_id == generate_table_id(full_table_name), 'Provided table_id does not match generated table_id.'

                restr = self & {'table_id': table_id}

                if len(restr) == 0:
                    self.insert1(
                        dict(
                            table_id=table_id, 
                            full_table_name=full_table_name, 
                            exists=1,
                            djp_version=version
                            ),
                        )
                if len(restr) == 1:
                    restr._update('exists', 1)
      
            else:
                table_id_restr = f'table_id LIKE "{table_id}%"' if table_id is not None else None
                full_table_name_restr = {'full_table_name': full_table_name} if full_table_name is not None else None
                
                restr = [r for r in [table_id_restr, full_table_name_restr] if r is not None]
                restr = self & dj.AndList(restr)

                if action == 'delete':
                    assert ~((table_id is None) and (full_table_name is None)), 'Provide table_id or full_table_name to delete.'
                    assert len(restr) == 1, 'There should be only one entry to delete.'
                    restr._update('exists', 0)
                    return

                if (table_id is None) and (full_table_name is None):
                    return self

                table_id, full_table_name = restr.fetch1('table_id', 'full_table_name')
                table = goto(table_id, directory=directory)
                return table

        except Exception as e:
            logging.error(e)
            logging.info('failure interacting with ~tables')

    @property
    def exists(self):
        """Returns existing tables"""
        return FreeTable(self.connection, self.full_table_name) & dj.AndList([{'exists': 1}])

    @property
    def deleted(self):
        """Returns deleted tables"""
        return FreeTable(self.connection, self.full_table_name) & dj.AndList([{'exists': 0}])

    def delete(self):
        """bypass interactive prompts and cascading dependencies"""
        self.delete_quick()

    def drop(self):
        """bypass interactive prompts and cascading dependencies"""
        self.drop_quick()
