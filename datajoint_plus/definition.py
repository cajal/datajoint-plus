import re
import inspect
from .logging import getLogger
import datajoint as dj

from datajoint_plus.utils import unwrap

logger = getLogger(__name__)

class StrToTable:
    """
    Abstract class -1
    """ 
    def __init__(self, source, context=None, **kwargs):
        self.source = source
        self.declaration_context = context or inspect.currentframe().f_back.f_globals
        self.kwargs = kwargs
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def table(self):
        try:
            table = eval(self.source, self.declaration_context)
            if inspect.isclass(table):
                return table()
            else:
                return table
        except:
            msg = f'Unable to instantiate {self.__class__.__qualname__}.'
            logger.exception(msg)
            raise NotImplementedError(msg)
    
    @property
    def base(self):
        b = self.table
        try:
            while not isinstance(b, dj.user_tables.UserTable):
                b = getattr(b, '_arg')
        except:
            msg = f'Unable to extract base table of type {self.table.__class__.__qualname__}.'
            logger.error(msg)
            raise NotImplementedError(msg)
        return b
    
    def __repr__(self):
        return f'{self.__class__.__qualname__}({self.source})'
    
    def __call__(self):
        return self.table

class Base:
    def __init__(self, source, type):
        if issubclass(source.__class__, type):
            self.__dict__ = source.__dict__
        else:
            source = unwrap(source)
            assert isinstance(source, str), f'Source must be of type {type.__name__} or str, not {source.__class__.__name__}.'
            self.source = source

class Definition:
    def __init__(self, source):
        if issubclass(source.__class__, __class__):
            self.__dict__ = source.__dict__
    
    def make_definition(self):
        for d in self._definition:
            foreign_key = {'primary': [], 'secondary': []}
            for ps in ['primary', 'secondary']:
                for emd_type in ['entities', 'methods', 'destinations']:
                    emd_nt = getattr(self, emd_type)
                    for emd in emd_nt:
                        if emd.inheritance == ps:
                            foreign_key[ps].extend(f"-> self.str_to_base(**{{'emd_type': '{emd_type}', 'source': '{emd.source}', 'context': self.declaration_context}}) \n") 
            
            definition = ''.join([
                "-> master \n",
                "".join(foreign_key['primary']),
                """
                ---
                """,
                "".join(foreign_key['secondary'])
            ])
            if self._append_timestamp_to_definition:
                definition += f"""
                {self._timestamp_name}=CURRENT_TIMESTAMP: timestamp # 
                """
            
            # format definition
            f_definition = []
            for line in re.split("\n", definition):
                line = line.strip(' ')
                if line:
                    f_definition.append(line)
            self.definition = '\n '.join(f_definition)
        
        return definition

    @property
    def definition(self):
        return self.make_definition()

    def __call__(self):
        return self.definition()

    class ForeignKey(Base):
        def __init__(self, source):
            super().__init__(source, __class__)

    
    class Attribute(Base):
        pass

    
    # Aliases
    Fk = ForeignKey
    At = Attribute
