"""
Extensions of DataJoint Table
"""
import inspect
import numpy as np
import uuid
import collections
import pandas
import itertools
from pathlib import Path

from .logging import getLogger

import datajoint as dj
from datajoint.errors import (
    DuplicateError,
    DataJointError,
    UnknownAttributeError
)
from datajoint.expression import QueryExpression
from datajoint_plus import blob
from datajoint_plus.hash import generate_table_id

from .utils import classproperty, goto
from .version import __version__ as version

logger = getLogger(__name__)


class Table(dj.table.Table):
    """
    Extensions to DataJoint Table
    """
    
    _table_log_ = None

    def declare(self, context=None):
        super().declare(context=context)
        if not isinstance(self, TableLog):
            self._table_log(self.table_id, self.full_table_name, action='add')
    
    def drop(self):
        dj.table.Table.drop(self)
        self._table_log(self.table_id, action='delete')

    def drop_quick(self):
        dj.table.Table.drop_quick(self)
        self._table_log(self.table_id, action='delete')

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
            except DataJointError:
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
                except DataJointError:
                    raise DataJointError('If return_free_table=True and full_table_name is not provided, table_id and ftn_lookup must be provided.')
            return FreeTable(self.connection, full_table_name)
        
        else:
            return goto(table_id=table_id, full_table_name=full_table_name, directory=directory)

    @property
    def _table_log(self):
        if self._table_log_ is None:
            self._table_log_ = TableLog(self.connection, database=self.database)
        return self._table_log_

    @classproperty
    def table_id(cls):
        return generate_table_id(cls.full_table_name)

    def insert(self, rows, replace=False, skip_duplicates=False, ignore_extra_fields=False, allow_direct_insert=None):
        """
        Insert a collection of rows.

        :param rows: An iterable where an element is a numpy record, a dict-like object, a pandas.DataFrame, a sequence,
            or a query expression with the same heading as table self.
        :param replace: If True, replaces the existing tuple.
        :param skip_duplicates: If True, silently skip duplicate inserts.
        :param ignore_extra_fields: If False, fields that are not in the heading raise error.
        :param allow_direct_insert: applies only in auto-populated tables.
                                    If False (default), insert are allowed only from inside the make callback.

        Example::
        >>> relation.insert([
        >>>     dict(subject_id=7, species="mouse", date_of_birth="2014-09-01"),
        >>>     dict(subject_id=8, species="mouse", date_of_birth="2014-09-02")])
        """

        if isinstance(rows, pandas.DataFrame):
            # drop 'extra' synthetic index for 1-field index case -
            # frames with more advanced indices should be prepared by user.
            rows = rows.reset_index(
                drop=len(rows.index.names) == 1 and not rows.index.names[0]
            ).to_records(index=False)

        # prohibit direct inserts into auto-populated tables
        if not allow_direct_insert and not getattr(self, '_allow_insert', True):  # allow_insert is only used in AutoPopulate
            raise DataJointError(
                'Inserts into an auto-populated table can only done inside its make method during a populate call.'
                ' To override, set keyword argument allow_direct_insert=True.')

        heading = self.heading
        if inspect.isclass(rows) and issubclass(rows, QueryExpression):   # instantiate if a class
            rows = rows()
        if isinstance(rows, QueryExpression):
            # insert from select
            if not ignore_extra_fields:
                try:
                    raise DataJointError(
                        "Attribute %s not found. To ignore extra attributes in insert, set ignore_extra_fields=True." %
                        next(name for name in rows.heading if name not in heading))
                except StopIteration:
                    pass
            fields = list(name for name in rows.heading if name in heading)
            query = '{command} INTO {table} ({fields}) {select}{duplicate}'.format(
                command='REPLACE' if replace else 'INSERT',
                fields='`' + '`,`'.join(fields) + '`',
                table=self.full_table_name,
                select=rows.make_sql(select_fields=fields),
                duplicate=(' ON DUPLICATE KEY UPDATE `{pk}`={table}.`{pk}`'.format(
                    table=self.full_table_name, pk=self.primary_key[0])
                           if skip_duplicates else ''))
            self.connection.query(query)
            return

        if heading.attributes is None:
            logger.warning('Could not access table {table}'.format(table=self.full_table_name))
            return

        field_list = None  # ensures that all rows have the same attributes in the same order as the first row.

        def make_row_to_insert(row):
            """
            :param row:  A tuple to insert
            :return: a dict with fields 'names', 'placeholders', 'values'
            """
            def make_placeholder(name, value):
                """
                For a given attribute `name` with `value`, return its processed value or value placeholder
                as a string to be included in the query and the value, if any, to be submitted for
                processing by mysql API.
                :param name:  name of attribute to be inserted
                :param value: value of attribute to be inserted
                """
                if ignore_extra_fields and name not in heading:
                    return None
                attr = heading[name]
                if attr.adapter:
                    value = attr.adapter.put(value)
                if value is None or (attr.numeric and (value == '' or np.isnan(float(value)))):
                    # set default value
                    placeholder, value = 'DEFAULT', None
                else:  # not NULL
                    placeholder = '%s'
                    if attr.uuid:
                        if not isinstance(value, uuid.UUID):
                            try:
                                value = uuid.UUID(value)
                            except (AttributeError, ValueError):
                                raise DataJointError(
                                    'badly formed UUID value {v} for attribute `{n}`'.format(v=value, n=name)) from None
                        value = value.bytes
                    elif attr.is_blob:
                        value = blob.pack(value)
                        value = self.external[attr.store].put(value).bytes if attr.is_external else value
                    elif attr.is_attachment:
                        attachment_path = Path(value)
                        if attr.is_external:
                            # value is hash of contents
                            value = self.external[attr.store].upload_attachment(attachment_path).bytes
                        else:
                            # value is filename + contents
                            value = str.encode(attachment_path.name) + b'\0' + attachment_path.read_bytes()
                    elif attr.is_filepath:
                        value = self.external[attr.store].upload_filepath(value).bytes
                    elif attr.numeric:
                        value = str(int(value) if isinstance(value, bool) else value)
                return name, placeholder, value

            def check_fields(fields):
                """
                Validates that all items in `fields` are valid attributes in the heading
                :param fields: field names of a tuple
                """
                if field_list is None:
                    if not ignore_extra_fields:
                        for field in fields:
                            if field not in heading:
                                raise KeyError(u'`{0:s}` is not in the table heading'.format(field))
                elif set(field_list) != set(fields).intersection(heading.names):
                    raise DataJointError('Attempt to insert rows with different fields')

            if isinstance(row, np.void):  # np.array
                check_fields(row.dtype.fields)
                attributes = [make_placeholder(name, row[name])
                              for name in heading if name in row.dtype.fields]
            elif isinstance(row, collections.abc.Mapping):  # dict-based
                check_fields(row)
                attributes = [make_placeholder(name, row[name]) for name in heading if name in row]
            else:  # positional
                try:
                    if len(row) != len(heading):
                        raise DataJointError(
                            'Invalid insert argument. Incorrect number of attributes: '
                            '{given} given; {expected} expected'.format(
                                given=len(row), expected=len(heading)))
                except TypeError:
                    raise DataJointError('Datatype %s cannot be inserted' % type(row))
                else:
                    attributes = [make_placeholder(name, value) for name, value in zip(heading, row)]
            if ignore_extra_fields:
                attributes = [a for a in attributes if a is not None]

            assert len(attributes), 'Empty tuple'
            row_to_insert = dict(zip(('names', 'placeholders', 'values'), zip(*attributes)))
            nonlocal field_list
            if field_list is None:
                # first row sets the composition of the field list
                field_list = row_to_insert['names']
            else:
                #  reorder attributes in row_to_insert to match field_list
                order = list(row_to_insert['names'].index(field) for field in field_list)
                row_to_insert['names'] = list(row_to_insert['names'][i] for i in order)
                row_to_insert['placeholders'] = list(row_to_insert['placeholders'][i] for i in order)
                row_to_insert['values'] = list(row_to_insert['values'][i] for i in order)

            return row_to_insert

        rows = list(make_row_to_insert(row) for row in rows)
        if rows:
            try:
                query = "{command} INTO {destination}(`{fields}`) VALUES {placeholders}{duplicate}".format(
                    command='REPLACE' if replace else 'INSERT',
                    destination=self.from_clause,
                    fields='`,`'.join(field_list),
                    placeholders=','.join('(' + ','.join(row['placeholders']) + ')' for row in rows),
                    duplicate=(' ON DUPLICATE KEY UPDATE `{pk}`=`{pk}`'.format(pk=self.primary_key[0])
                               if skip_duplicates else ''))
                self.connection.query(query, args=list(
                    itertools.chain.from_iterable((v for v in r['values'] if v is not None) for r in rows)))
            except UnknownAttributeError as err:
                raise err.suggest('To ignore extra fields in insert, set ignore_extra_fields=True') from None
            except DuplicateError as err:
                raise err.suggest('To ignore duplicate entries in insert, set skip_duplicates=True') from None

    def _update(self, attrname, value=None):
        """
            Updates a field in an existing tuple. This is not a datajoyous operation and should not be used
            routinely. Relational database maintain referential integrity on the level of a tuple. Therefore,
            the UPDATE operator can violate referential integrity. The datajoyous way to update information is
            to delete the entire tuple and insert the entire update tuple.

            Safety constraints:
               1. self must be restricted to exactly one tuple
               2. the update attribute must not be in primary key

            Example:
            >>> (v2p.Mice() & key).update('mouse_dob', '2011-01-01')
            >>> (v2p.Mice() & key).update( 'lens')   # set the value to NULL
        """
        if len(self) != 1:
            raise DataJointError('Update is only allowed on one tuple at a time')
        if attrname not in self.heading:
            raise DataJointError('Invalid attribute name')
        if attrname in self.heading.primary_key:
            raise DataJointError('Cannot update a key value.')

        attr = self.heading[attrname]

        if attr.is_blob:
            value = blob.pack(value)
            placeholder = '%s'
        elif attr.numeric:
            if value is None or np.isnan(float(value)):  # nans are turned into NULLs
                placeholder = 'NULL'
                value = None
            else:
                placeholder = '%s'
                value = str(int(value) if isinstance(value, bool) else value)
        else:
            placeholder = '%s' if value is not None else 'NULL'
        command = "UPDATE {full_table_name} SET `{attrname}`={placeholder} {where_clause}".format(
            full_table_name=self.from_clause,
            attrname=attrname,
            placeholder=placeholder,
            where_clause=self.where_clause)
        self.connection.query(command, args=(value, ) if value is not None else ())


class FreeTable(Table, dj.FreeTable):
    """
    Extension to DataJoint FreeTable
    """
    
    def drop(self):
        dj.table.Table.drop(self)
        self._table_log(full_table_name = self.full_table_name, action='delete')

    def drop_quick(self):
        dj.table.Table.drop_quick(self)
        self._table_log(full_table_name = self.full_table_name, action='delete')


class TableLog(Table):
    """
    Log of tables in each schema.
    Instances are callable.  Calls with table hash return the table entry. 
    Calls with hash and action update the table.
    """

    def __init__(self, arg, database=None):
        super().__init__()

        if isinstance(arg, TableLog):
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
    
    @property
    def table_id(self):
        return generate_table_id(self.full_table_name)
    
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
            logger.exception(e)
            logger.info('failure interacting with ~tables')

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
