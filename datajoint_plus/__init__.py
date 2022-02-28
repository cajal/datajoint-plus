"""
DataJointPlus is an extension to DataJoint. DataJointPlus extends DataJoint's 
user tables with automatic hashing and enhances the DataJoint 
master-part relationship, providing templates for commonly used motifs.
"""

__all__ = ['__version__',
           'DataJointPlusModule', 'create_djp_module',
           'reassign_master_attribute', 'add_datajoint_plus',
           'Manual', 'Lookup', 'Imported', 'Computed', 'Part',
           'add_objects', 'check_if_latest_version', 'enable_datajoint_flags',
           'format_table_name', 'make_store_dict', 'register_externals', 
           'generate_hash', 'validate_and_generate_hash', 'parse_definition', 
           'reform_definition']

from .compatibility import (DataJointPlusModule, add_datajoint_plus,
                            reassign_master_attribute)
from .hash import generate_hash, validate_and_generate_hash
from .user_tables import Computed, Imported, Lookup, Manual, Part
from .utils import (add_objects, check_if_latest_version, enable_datajoint_flags,
                    format_table_name, make_store_dict, register_externals)
from .version import __version__
from .heading import parse_definition, reform_definition

create_djp_module = DataJointPlusModule # Aliases for DataJointPlusModule

check_if_latest_version()
