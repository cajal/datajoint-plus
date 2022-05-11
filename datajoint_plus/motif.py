"""
User tables for DataJointPlus Motifs
"""
from collections import namedtuple
import inspect
from .logging import getLogger
import re
import datajoint as dj
import numpy as np
from datajoint_plus.definition import StrToTable
from datajoint_plus.errors import MakerDestinationError, MakerError, MakerInputError, MakerMethodError
from datajoint.fetch import is_key
from datajoint.expression import Projection
from datajoint_plus.user_tables import UserTable

from .base import BaseMaster, BasePart
from .utils import classproperty, safedict, unwrap, wrap

logger = getLogger(__name__)






class Entity(StrToTable):
    is_entity = True
    def __init__(self, source, add_to_key_source:bool=True, inheritance:bool='primary', get=None, context=None, **kwargs):
        if getattr(source, 'is_entity', False):
            self.__dict__ = source.__dict__
        else:
            if is_key(get):
                self.get = self.get_key
            super().__init__(source=source, add_to_key_source=add_to_key_source, inheritance=inheritance, get=get, context=context, **kwargs)
    
    def get_key(self, key):
        return (self.table & key).fetch1('KEY')
    
    
class Method(StrToTable):
    is_method = True
    def __init__(self, source, add_to_key_source:bool=True, inheritance:bool='primary', run=None, context=None, **kwargs):
        if getattr(source, 'is_method', False):
            self.__dict__ = source.__dict__
        else:
            super().__init__(source=source, add_to_key_source=add_to_key_source, inheritance=inheritance, run=run, context=context, **kwargs)

        
class Destination(StrToTable):
    is_destination = True
    def __init__(self, source, inheritance:bool='primary', put=None, context=None, **kwargs):
        if getattr(source, 'is_destination', False):
            self.__dict__ = source.__dict__
        else:
            super().__init__(source=source, inheritance=inheritance, put=put, context=context, **kwargs)


class Motif:
    """
    Class level: -2
    """

    @classmethod
    def _init_validation(cls, **kwargs):
        if (cls.hash_name is None) and (cls.lookup_name is None):
            raise NotImplementedError('Subclasses of Motif must implement "lookup_name" or "hash_name".')
    
    @staticmethod
    def _emd_mapping(emd_type:str):
        prefix_mapping = {
            'entities': 'get',
            'methods': 'run',
            'destinations': 'put'
        }

        cls_mapping = {
            'entities': Entity,
            'methods': Method,
            'destinations': Destination,
        }
        try:
            return prefix_mapping[emd_type], cls_mapping[emd_type]
        except KeyError:
            msg = f'emd_type {emd_type} not recognized.'
            logger.error(msg)
            raise AttributeError(msg)
    
    @classmethod
    def _emd_namedtuple(cls, emd_type:str, context=None):
        emd_prefix, emd_cls = cls._emd_mapping(emd_type)
        emd = getattr(cls, emd_prefix + '_' + emd_type, None)
        if emd is not None:
            emds = []
            for source in wrap(emd):
                emds.append(
                    emd_cls(
                        source, 
                        context=context
                    )
                )
        if emds is not None:
            nt = namedtuple(
                emd_type, 
                field_names=[getattr(t.table, 'class_name_valid_id', getattr(t.table, 'class_name')) for t in emds]
            )
            nt.__repr__ = cls.nt_repr
            return nt(*emds)
        else:
            return namedtuple(emd_type, field_names='')()
    
    @classmethod
    def str_to_table(cls, source:str, context=None):
        return StrToTable(source, context=context).table

    @classmethod
    def str_to_base(cls, source:str, context=None):
        return StrToTable(source, context=context).base


class EntityLookup(Motif, BaseMaster, UserTable, dj.Lookup):
    @classmethod
    def _init_validation(cls, **kwargs):
        super(Motif, cls)._init_validation(**kwargs)
        super()._init_validation(**kwargs)
    
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)
    
    def get(self, key={}, attrs={}):
        if self.stores is None:
            return super().get(key=key, attrs=attrs)
        else:
            data = []        
            for store in wrap(self.stores):
                store = eval(store)
                if store.full_table_name in self.descendants():
                    key = (self & key).proj(**{store.lookup_name: self.lookup_name}).fetch('KEY')
                else:
                    raise
                d = store().get(key=key, attrs=attrs)
                data.append(d)
            return unwrap(data)


class MethodLookup(Motif, BaseMaster, UserTable, dj.Lookup):
    @classmethod
    def _init_validation(cls, **kwargs):
        super(Motif, cls)._init_validation(**kwargs)
        super()._init_validation(**kwargs)
    
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)

    def run(self, **kwargs):
        return self.r1p(kwargs).run(**kwargs)


class StoreLookup(Motif, BaseMaster, UserTable, dj.Lookup):    
    @classmethod
    def _init_validation(cls, **kwargs):
        super(Motif, cls)._init_validation(**kwargs)
        super()._init_validation(**kwargs)
    
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)
        
    def get(self, key, **kwargs):
        return self.r1p(key).get(**kwargs)
    
    def put(self, parts, **kwargs):
        for p in parts:
            p.put(**kwargs)


class MakerLookup(Motif, BaseMaster, UserTable, dj.Lookup):
    @classmethod
    def _init_validation(cls, **kwargs):
        super(Motif, cls)._init_validation(**kwargs)
        super()._init_validation(**kwargs)
    
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)
    
    @classmethod
    def populate(cls, *args, **kwargs):
        for p in cls.parts(as_cls=True):
            p.populate(*args, **kwargs)


class NestedMethod(Motif, BasePart, UserTable, dj.Part):
    @classmethod
    def _init_validation(cls, **kwargs):
        super(Motif, cls)._init_validation(**kwargs)
        super()._init_validation(**kwargs)
        
    def __init_subclass__(cls, **kwargs):
        cls.enable_hashing = True
        cls._init_validation(**kwargs)


class NestedStore(Motif, BasePart, UserTable, dj.Part):
    @classmethod
    def _init_validation(cls, **kwargs):
        super(Motif, cls)._init_validation(**kwargs)
        super()._init_validation(**kwargs)
    
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)


class NestedMaker(Motif, BasePart, UserTable, dj.Part, dj.Computed):
    definition = None
    get_entities = None
    run_methods = None
    put_destinations = None
    _dict_merge_warn_overwrite = True
    _dict_merge_allow_overwrite = False
    _append_timestamp_to_definition = True
    _timestamp_name = 'ts_inserted'
    
    def _init_validation(cls, **kwargs):
        super(Motif, cls)._init_validation(**kwargs)
        super()._init_validation(**kwargs)

    def __init_subclass__(cls, **kwargs):
        # HASHED ATTRS
        if getattr(cls, 'hashed_attrs', None) == 'key_source':
            cls.hashed_attrs = cls.key_source.primary_key
           
        # DEFINITION
        # make definition
        if cls.definition is None:
            # sort dependencies
            foreign_key = {'primary': [], 'secondary': []}
            for ps in ['primary', 'secondary']:
                for emd_type in ['entities', 'methods', 'destinations']:
                    emd_nt = getattr(cls, emd_type)
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
            if cls._append_timestamp_to_definition:
                definition += f"""
                {cls._timestamp_name}=CURRENT_TIMESTAMP: timestamp # 
                """
            
            # format definition
            f_definition = []
            for line in re.split("\n", definition):
                line = line.strip(' ')
                if line:
                    f_definition.append(line)
            cls.definition = '\n '.join(f_definition)
            
        cls._init_validation(cls, **kwargs)
    
    @classproperty
    def entities(cls):
        return cls._emd_namedtuple('entities', context=cls.declaration_context)
    
    @classproperty
    def methods(cls):
        return cls._emd_namedtuple('methods', context=cls.declaration_context)
    
    @classproperty
    def destinations(cls):
        return cls._emd_namedtuple('destinations', context=cls.declaration_context)
    
    @classproperty
    def upstream(cls):
        return namedtuple('upstream', ['entities', 'methods'])(cls.entities, cls.methods)
    
    @classproperty
    def downstream(cls):
        return namedtuple('downstream', ['destinations'])(cls.destinations)
    
    @classproperty
    def key_source(cls):
        ks = []
        for emd_type in ['get_entities', 'run_methods']:
            if getattr(cls, emd_type) is not None:
                items = getattr(cls, emd_type.split('_')[1])
                for i in items:
                    if i.add_to_key_source:
                        ks.append(i.table)
        ks = np.product(ks)
        assert isinstance(ks, dj.Table), 'key_source must be a DataJoint table. Check that get_entities and/ or run_methods was defined correctly.'
        return ks
    
    def _extract_fxn(self, emd):
        """
        Extracts the "get", "run" or "put" function from the input.
        """
        if getattr(emd, 'is_entity', False):
            fxn_type = 'get'
            err = MakerInputError
        elif getattr(emd, 'is_method', False):
            fxn_type = 'run'
            err = MakerMethodError
        elif getattr(emd, 'is_destination', False):
            fxn_type = 'put'
            err = MakerDestinationError
        else:
            raise AttributeError(f'type {type(emd)} not recognized. Expected djp.Entity, djp.Method or djp.Destination.')
        
        fxn = getattr(emd, fxn_type, None) or getattr(emd.table, fxn_type, None)

        if fxn is None:
            msg = f'"{fxn_type}" function not found for {emd}.'
            logger.error(msg)
            raise err(msg)
        return fxn
    
    @staticmethod
    def _validate_arg(arg):
        """
        Validates and returns arg, or errors if validation fails.
        """
        if not isinstance(arg, dict):
            msg = f'arg should be dict instance.'
            logger.error(msg)
            raise MakerError(msg)
        return arg
    
    def make(self, key):
        # GET ENTITIES
        inputs = safedict(warn=self._dict_merge_warn_overwrite, overwrite=self._dict_merge_allow_overwrite)
        if self.entities:
            for e in self.entities:
                get = self._extract_fxn(e)
                inp = self._validate_arg(get(key))
                inputs.update(**inp)
        inputs = {**key, **inputs}

        # RUN METHODS
        results = safedict(warn=self._dict_merge_warn_overwrite, overwrite=self._dict_merge_allow_overwrite)
        if self.methods:
            for m in self.methods:
                run = self._extract_fxn(m)
                res = self._validate_arg(run(**inputs))
                results.update(**res)
        results = {**key, **results}
        
        # HASH
        hash_dict = {self.hash_name: self.hash1(results)} if self.enable_hashing else {}
        row = {**hash_dict, **results}

        # PUT DESTINATIONS
        if self.destinations:
            for d in self.destinations:
                put = self._extract_fxn(d)
                put(**row)

        self.put(**row, skip_hashing=True, insert_to_master=True, insert_to_master_kws={'ignore_extra_fields': True, 'skip_duplicates': True})
    
    def nt_repr(self):
        base = self.__class__.__name__ + '('
        for i, f in enumerate(self._fields):
            try:
                self._fields[i+1]
            except IndexError:
                base += f + ')'
            else:
                base += f + ', '
        return base