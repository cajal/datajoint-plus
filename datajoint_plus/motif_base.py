"""
Motif tables for DataJointPlus
"""

from datajoint_plus.user_tables import UserTable
from datajoint_plus.utils import classproperty
import datajoint as dj


class Motif(UserTable):
    @classmethod
    def is_motif_table(cls):
        return issubclass(Motif)


class MotifMaster(Motif):
    @classproperty
    def definition(cls):
        return ''.join([f"""
        #{cls().class_name}
        """,
        f"""
        {cls.hash_name} : varchar({cls.hash_len}) # hash
        """
        ,
        """
        ---
        lookup_table=NULL : varchar(32) # (optional) table_id with information about hash
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """
        ])


class Nested(Motif):
    @classmethod
    def is_nested(cls):
        return issubclass(cls, Nested)

    def delete_from_master(self):
        # with dj.conn().transaction:
        keys = self.fetch('KEY')
        (self.master & keys).delete()
            
    def delete_quick_from_master(self):
        # with dj.conn().transaction:
        keys = self.fetch('KEY')
        self.delete_quick()
        (self.master & keys).delete_quick()
            
    def delete_quick(self, delete_from_master=True):
            if delete_from_master:
                self.delete_quick_from_master()
            else:
                super().delete_quick()
        
    def delete(self, delete_from_master=True):
        if delete_from_master:
            self.delete_from_master()
        else:
            super().delete()

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



