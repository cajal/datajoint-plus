"""
Hosts the original DataJoint table tiers extended with DataJointPlus.
"""
import datajoint as dj
from datajoint.user_tables import UserTable

from .base import MasterBase, PartBase
from .table import Tables


class UserTable(UserTable):
    def declare(self, context=None):
        super().declare(context=context)
        self._tables(self.table_id, self.full_table_name, action='add')
    
    def drop_quick(self):
        super().drop_quick()
        self._tables(self.table_id, action='delete')

    @property
    def _tables(self):
        if self._tables_ is None:
            self._tables_ = Tables(self.connection, database=self.database)
        return self._tables_

    

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
