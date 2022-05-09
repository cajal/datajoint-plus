"""
Tools to enhance compatability with DataJoint.
"""

import inspect

import datajoint as dj
from datajoint.user_tables import UserTable

from .base import Base
from .logging import getLogger
from .user_tables import Computed, Imported, Lookup, Manual, Part

logger = getLogger(__name__)

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
            logger.exception(f'Could not add DataJointPlus to {name}.')


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


