"""
Base motif classes 
"""
from .logging import getLogger
import inspect
from .base import Base
from datajoint.expression import Projection
from .utils import classproperty

logger = getLogger(__name__)


class StrToTable:
    """
    Abstract class -1
    """
    def __init__(self, **kwargs):
        if isinstance(self.table, Base):
            self.base_table = self.table.__class__()
            
        elif isinstance(self.table, Projection):
            self.base_table = self.table._arg.__class__()
        
        else:
            msg = f'Unable to instantiate table of type {self.table.__class__.__qualname__}.'
            logger.error(msg)
            raise NotImplementedError(msg)
        
    @property
    def table(self):
        try:
            table = eval(self._table, self.declaration_context)
            if inspect.isclass(table):
                return table()
            else:
                return table
        except:
            msg = f'Unable to instantiate {self.__class__.__qualname__}.'
            logger.exception(msg)
            raise NotImplementedError(msg)
            
    def __repr__(self):
        return f'{self.__class__.__qualname__}({self._table})'
    
    def __call__(self):
        return self.table


class Motif:
    """
    Class level: -2
    """

    @classmethod
    def _init_validation(cls, **kwargs):
        if (cls.hash_name is None) and (cls.lookup_name is None):
            raise NotImplementedError('Subclasses of Motif must implement "lookup_name" or "hash_name".')

    @classproperty
    def lookup_name(cls):
        return cls.hash_name if cls.hash_name is not None else logger.error('"lookup_name" not defined.')

    @classmethod
    def lookup_as(cls, table):
        return cls.proj(..., **{table.lookup_name: cls.lookup_name})



