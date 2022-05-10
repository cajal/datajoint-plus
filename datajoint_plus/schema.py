"""
DataJointPlus Schema extensions
"""

from .logging import getLogger
import types

import datajoint as dj
from .compatibility import add_datajoint_plus
from .utils import enable_datajoint_flags, load_dependencies, register_externals, split_full_table_name, reform_full_table_name
from .table import TableLog
from .hash import generate_table_id
from .utils import classproperty
from .table import FreeTable

logger = getLogger(__name__)

class Schema(dj.Schema):
    """
    Extension of dj.Schema that adds a table log

    Additional params:
    :param load_dependencies (bool): Loads the DataJoint graph.
    """
    def __init__(self, schema_name, context=None, load_dependencies=True, *, connection=None, create_schema=True, create_tables=True):
        super().__init__(schema_name=schema_name, context=context, connection=connection, create_schema=create_schema, create_tables=create_tables)

        # attempt to update ~tables
        try:
            self._tables = None
            for table_name in self.list_tables():
                full_table_name = reform_full_table_name(self.database, table_name)
                self.tables(generate_table_id(full_table_name), full_table_name, action='add')
            for key in self._tables:
                _, name = split_full_table_name(key['full_table_name'])
                if name not in self.list_tables():
                    self.tables(full_table_name=key['full_table_name'], action='delete')
        except:
            pass

        self.load_dependencies(force=load_dependencies, verbose=False)


    @classproperty
    def is_schema(cls):
        True

    @property
    def tables(self):
        if self._tables is None:
            self._tables = TableLog(self.connection, self.database)
        return self._tables

    def free_table(self, table_name=None, full_table_name=None):
        """
        Generates a free table in the schema from table_name or full_table_name.
        
        :param table_name: (str) Name of table in MySQL. 
        :param full_table_name: (str) Full table name with format '`database`.`table_name`'

        :returns: dj.FreeTable
        """
        table_not_in_schema = 'Table not found in schema. See tables with schema.list_tables().'
        assert (table_name is None) ^ (full_table_name is None), 'Provide table_name or full_table_name but not both'
        if table_name is not None:
            assert table_name in self.list_tables(), table_not_in_schema
            full_table_name = reform_full_table_name(self.database, table_name)
        elif full_table_name is not None:
            try:
                database, _ = split_full_table_name(full_table_name)
            except ValueError:
                raise ValueError('Split full_table_name failed. Did you pass a table_name in full_table_name arg?')
            assert database == self.database, table_not_in_schema
        else:
            raise AttributeError('Provide table_name or full_table_name.')
        return FreeTable(self.connection, full_table_name)
    
    def load_dependencies(self, force=True, verbose=True):
        """
        Loads dependencies into DataJoint networkx graph. 
        """
        load_dependencies(self.connection, force=force, verbose=verbose)


class VirtualModule(types.ModuleType):
    """
    A virtual module which will contain context for schema.
    """
    def __init__(self, module_name, schema_name, *, create_schema=False,
                 create_tables=False, connection=None, add_objects=None):
        """
        Creates a python module with the given name from the name of a schema on the server and
        automatically adds classes to it corresponding to the tables in the schema.
        :param module_name: displayed module name
        :param schema_name: name of the database in mysql
        :param create_schema: if True, create the schema on the database server
        :param create_tables: if True, module.schema can be used as the decorator for declaring new
        :param connection: a dj.Connection object to pass into the schema
        :param add_objects: additional objects to add to the module
        :return: the python module containing classes from the schema object and the table classes
        """
        super(VirtualModule, self).__init__(name=module_name)
        _schema = Schema(schema_name, create_schema=create_schema, create_tables=create_tables,
                         connection=connection)
        if add_objects:
            self.__dict__.update(add_objects)
        self.__dict__['schema'] = _schema
        _schema.spawn_missing_classes(context=self.__dict__)


class DataJointPlusModule(VirtualModule):
    """
    DataJointPlus extension of DataJoint virtual module with the added ability to instantiate from an existing module.
    """
    def __init__(self, module_name=None, schema_name=None, module=None, schema_obj_name=None, add_externals=None, add_objects=None, create_schema=False, create_tables=False, connection=None, spawn_missing_classes=True, load_dependencies=True, enable_dj_flags=True, warn=True):
        """
        Add DataJointPlus methods to all DataJoint user tables in a DataJoint virtual module or to an existing module. 
        
        To instantiate a DataJoint Virtual Module, provide args module_name and schema_name. 
        
        To modify an existing module, provide arg module. 
        
        :param module_name (str): displayed module name (if using DataJoint Virtual module)
        :param schema_name (str): name of the database in mysql
        :param module (module): module to modify
        :param schema_obj_name (str): The name of the schema object you wish to instantiate (only needed if the module contains more than one DataJoint dj.schema object)
        :param add_externals (dict): Dictionary mapping to external files.
        :param add_objects (dict): additional objects to add to the module
        :param spawn_missing_classes (bool): Only relevant if module provided. If True, adds DataJoint tables not in module but present in mysql as classes. 
        :param load_dependencies (bool): Loads the DataJoint graph.
        :param create_schema (bool): if True, create the schema on the database server
        :param create_tables (bool): if True, module.schema can be used as the decorator for declaring new
        :param connection (dj.Connection): a dj.Connection object to pass into the schema
        :param enable_dj_flags (bool): If true runs djp.enable_datajoint_flags. May be necessary to use adapters. 
        :param warn (bool): if False, warnings are disabled. 
        :return: the virtual module or modified module with DataJointPlus added.
        """
        if schema_name:
            assert not module, 'Provide either schema_name or module but not both.'
            super().__init__(module_name=module_name if module_name else schema_name, schema_name=schema_name, add_objects=add_objects, create_schema=create_schema, create_tables=create_tables, connection=connection)
            
            self.load_dependencies(self.__dict__['schema'].connection, load_dependencies, verbose=False)
            
        elif module:
            super(dj.VirtualModule, self).__init__(name=module.__name__)
            if module_name:
                if warn:
                    logger.warning('module_name ignored when instantiated with module.')
                
            if schema_obj_name:
                assert schema_obj_name in module.__dict__, f'schema_obj_name: {schema_obj_name} not found in module.'
                schema_obj = module.__dict__[schema_obj_name]
                assert isinstance(schema_obj, dj.Schema), f'schema object should be of type {dj.Schema} not {type(schema_obj)}.'
            else:
                schemas = {k: {'obj': v, 'database': v.database} for k, v in module.__dict__.items() if isinstance(v, dj.Schema)}
                assert len(schemas.keys())==1, f"Found multiple schema objects with names {list(schemas.keys())}, mapping to respective databases {[v['database'] for v in schemas.values()]}. Specify the name of the schema object to instantiate with arg schema_obj_name." 
                schema_obj = list(schemas.values())[0]['obj']
            
            self.__dict__.update(module.__dict__)
            self.__dict__['schema'] = schema_obj
            
            if spawn_missing_classes:
                schema_obj.spawn_missing_classes(context=self.__dict__)
                
            self.load_dependencies(schema_obj.connection, load_dependencies, verbose=False)
                
            if add_objects:
                self.__dict__.update(add_objects)
        
        else:
            raise ValueError('Provide schema_name or module.')      
        
        if add_externals:
            register_externals(add_externals)
        
        if enable_dj_flags:
            enable_datajoint_flags()
            
        add_datajoint_plus(self)

    def load_dependencies(self, connection, force=True, verbose=True):
        """
        Loads dependencies into DataJoint networkx graph. 
        """
        load_dependencies(connection=connection, force=force, verbose=verbose)