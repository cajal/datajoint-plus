"""
Hosts the original DataJoint table tiers extended with DataJointPlus.
"""
from .logging import getLogger
import re

import datajoint as dj

from .table import Table
from .base import BaseMaster, BasePart

master_classes = (dj.Manual, dj.Lookup, dj.Computed, dj.Imported,)
part_classes = (dj.Part,)

logger = getLogger(__name__)

class UserTable(Table, dj.user_tables.UserTable):
    @property
    def is_user_table(self):
        return True

    @classmethod
    def is_master(cls):
        try:
            next(tier for tier in master_classes
                    if re.fullmatch(tier.tier_regexp, cls.table_name))
            return True
        except StopIteration:
            return False

    @classmethod
    def is_part(cls):
        try:
            next(tier for tier in part_classes
                        if re.fullmatch(tier.tier_regexp, cls.table_name))
            return True
        except StopIteration:
            return False


class Lookup(BaseMaster, UserTable, dj.Lookup):
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)
    

class Manual(BaseMaster, UserTable, dj.Manual):
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)


class Computed(BaseMaster, UserTable, dj.Computed):
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)


class Imported(BaseMaster, UserTable, dj.Imported):
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)


class Part(BasePart, UserTable, dj.Part):
    
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)

    def drop(self, force=False):
        if force:
            super(UserTable, self).drop()
        else:
            raise dj.DataJointError('Cannot drop a Part directly.  Delete from master instead')

    def drop_quick(self, force=False):
        if force:
            return super(UserTable, self).drop_quick()
        else:
            raise dj.DataJointError('Cannot drop a Part directly.  Delete from master instead')