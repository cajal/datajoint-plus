"""
Hosts the original DataJoint table tiers extended with DataJointPlus.
"""
import re

import datajoint as dj
from datajoint.user_tables import UserTable

from .base import MasterBase, PartBase
from .table import Tables
from .utils import classproperty

master_classes = (dj.Manual, dj.Lookup, dj.Computed, dj.Imported,)
part_classes = (dj.Part,)

class UserTable(UserTable):
    """
    Extensions to DataJoint UserTable
    """
    
    _tables_ = None

    def declare(self, context=None):
        super().declare(context=context)
        self._tables(self.table_id, self.full_table_name, action='add')
    
    def drop(self):
        super().drop()
        self._tables(self.table_id, action='delete')

    def drop_quick(self):
        super().drop_quick()
        self._tables(self.table_id, action='delete')

    @property
    def _tables(self):
        if self._tables_ is None:
            self._tables_ = Tables(self.connection, database=self.database)
        return self._tables_

    @classproperty
    def is_master(cls):
        try:
            next(tier for tier in master_classes
                    if re.fullmatch(tier.tier_regexp, cls.table_name))
            return True
        except StopIteration:
            return False

    @classproperty
    def is_part(cls):
        try:
            next(tier for tier in master_classes
                        if re.fullmatch(tier.tier_regexp, cls.table_name))
            return True
        except StopIteration:
            return False
    

class Lookup(MasterBase, UserTable, dj.Lookup):
    pass


class Manual(MasterBase, UserTable, dj.Manual):
    pass


class Computed(MasterBase, UserTable, dj.Computed):
    pass


class Imported(MasterBase, UserTable, dj.Imported):
    pass


class Part(PartBase, UserTable, dj.Part):
    pass
