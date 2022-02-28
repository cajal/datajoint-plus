"""
Tools to enhance compatability with DataJoint.
"""

import inspect
import logging
import traceback

import datajoint as dj
from datajoint.user_tables import UserTable
from .base import Base
from .user_tables import Computed, Imported, Lookup, Manual, Part
from .utils import enable_datajoint_flags, register_externals

djp_mapping = {
    'Lookup': Lookup,
    'Manual': Manual,
    'Computed': Computed,
    'Imported': Imported,
    'Part': Part
}

def add_datajoint_plus(module):
    """
    Adds DataJointPlus recursively to DataJoint tables inside the module.
    """
    
    for name in dir(module):
        try:
            if name in ['key_source', '_master', 'master']:
                continue
            obj = getattr(module, name)
            if inspect.isclass(obj) and issubclass(obj, UserTable) and not issubclass(obj, Base):
                bases = []
                for b in obj.__bases__:
                    if issubclass(b, UserTable):
                        b = djp_mapping[b.__name__]
                    bases.append(b)
                obj.__bases__ = tuple(bases)
                obj.parse_hash_info_from_header()
                add_datajoint_plus(obj)
        except:
            logging.warning(f'Could not add DataJointPlus to: {name}.')
            traceback.print_exc()


def reassign_master_attribute(module):
    """
    Overwrite .master attribute in DataJoint part tables to map to master class from current module. This is required if the DataJoint table is inherited.
    """
    for name in dir(module):
        # Get DataJoint tables
        if inspect.isclass(getattr(module, name)) and issubclass(getattr(module, name), dj.Table):
            obj = getattr(module, name)
            for nested in dir(obj):
                # Get Part tables
                if inspect.isclass(getattr(obj, nested)) and issubclass(getattr(obj, nested), dj.Part):
                    setattr(getattr(obj, nested), '_master', obj)


class DataJointPlusModule(dj.VirtualModule):
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
            
            if load_dependencies:
                self.__dict__['schema'].connection.dependencies.load()
            
        elif module:
            super(dj.VirtualModule, self).__init__(name=module.__name__)
            if module_name:
                if warn:
                    logging.warning('module_name ignored when instantiated with module.')
                
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
                
            if load_dependencies:
                schema_obj.connection.dependencies.load()
                
            if add_objects:
                self.__dict__.update(add_objects)
        
        else:
            raise ValueError('Provide schema_name or module.')      
        
        if add_externals:
            register_externals(add_externals)
        
        if enable_dj_flags:
            enable_datajoint_flags()
            
        add_datajoint_plus(self)
