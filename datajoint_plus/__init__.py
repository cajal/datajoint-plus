"""
DataJointPlus is an extension to DataJoint. DataJointPlus extends DataJoint's 
user tables with automatic hashing and enhances the DataJoint 
master-part relationship, providing templates for commonly used motifs.
"""

__all__ = ['__version__',
           'kill', 'set_password', 'AndList', 
           'Not', 'U', 'Connection', 'conn', 
           'AttributeAdapter', 'MatCell', 'MatStruct', 
           'Diagram', 'DataJointError', 'key',
           'list_schemas', 'config',
           'DataJointPlusModule', 'create_djp_module',
           'reassign_master_attribute', 'add_datajoint_plus',
           'Lookup', 'Computed', 'Part', 'Manual',
           'add_objects', 'check_if_latest_version', 'enable_datajoint_flags',
           'format_table_name', 'split_full_table_name', 'make_store_dict', 'register_externals', 
           'generate_hash', 'validate_and_generate_hash', 'parse_definition', 
           'reform_definition', 'errors', 'free_table', 
           'basicConfig', 'getLogger', 'LogFileManager']



# from DataJoint
from datajoint.admin import kill, set_password
from datajoint.attribute_adapter import AttributeAdapter
from datajoint.blob import MatCell, MatStruct
from datajoint.connection import Connection, conn
from datajoint.diagram import Diagram
from datajoint.errors import DataJointError
from datajoint.expression import AndList, Not, U
from datajoint.fetch import key
from datajoint.schemas import VirtualModule as DataJointVirtualModule, list_schemas 

# from DataJointPlus
from . import errors
from .compatibility import add_datajoint_plus, reassign_master_attribute
from .config import config
from .hash import generate_hash, validate_and_generate_hash
from .heading import parse_definition, reform_definition
from .logging import LogFileManager, basicConfig, getLogger
from .schema import DataJointPlusModule, Schema
from .table import FreeTable as free_table
from .user_tables import Computed, Lookup, Part, Manual
from .utils import (add_objects, check_if_latest_version,
                    enable_datajoint_flags, format_table_name, make_store_dict,
                    register_externals, split_full_table_name)
from .version import __version__

# aliases
ERD = Di = Diagram                      
create_dj_virtual_module = DataJointVirtualModule
schema = Schema 
create_djp_module = DataJointPlusModule 

# version control
check_if_latest_version()

