"""
User tables for DataJointPlus Motifs
"""

from collections import namedtuple
import inspect

import datajoint as dj
import numpy as np

from datajoint_plus.utils import classproperty, format_table_name, wrap, split_full_table_name

from .base import BaseMaster, BasePart
from .motif_base import MotifMaster, Nested

### MASTER

class Entity(MotifMaster, BaseMaster, dj.Lookup):
    @classmethod
    def _init_validation(cls, **kwargs):
        assert getattr(cls, 'hash_name', None) is not None, f'Subclasses of {cls.__base__.__name__} must implement hash_name.'

        if cls._hash_len is None:
            cls._hash_len = 32 # default hash length
        else:
            if not (isinstance(cls.hash_len, int) and (cls._hash_len > 0 and cls._hash_len <= 32)):
                raise NotImplementedError('_hash_len attribute must be an integer within range: [1, 32].')

        super()._init_validation(**kwargs)

    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)

    @classmethod
    def is_entity(cls):
        return issubclass(cls, Entity)    
    
    @classmethod
    def stores(cls):
        stores = [c for c in cls.parts(as_cls=True) if getattr(c, 'is_nested_store', False)]
        store_names = [c.class_name.split('.')[1] for c in stores]
        return namedtuple('NestedStore', store_names)(*stores) 

    @classmethod
    def makers(cls):
        makers = [c for c in cls.parts(as_cls=True) if getattr(c, 'is_nested_maker', False)]
        maker_names = [c.class_name.split('.')[1] for c in makers]
        return namedtuple('NestedStore', maker_names)(*makers) 

    @classmethod
    def get(cls, key, **kwargs):
        return cls.r1p(key, include_parts=cls.stores(), **kwargs).get()
    
    @classmethod
    def get_with_hash(cls, hash, **kwargs):
        return cls.get({cls.hash_name: hash}, **kwargs)

    @classmethod
    def restrict_stores(cls, restr={}, **kwargs):
        return cls.restrict_parts(part_restr=restr, include_parts=cls.stores(), **kwargs)
    
    @classmethod
    def restrict_one_store(cls, restr={}, **kwargs):
        return cls.r1p(part_restr=restr, include_parts=cls.stores(), **kwargs)
        
    @classmethod
    def restrict_one_store_with_hash(cls, hash, hash_name=None, **kwargs):
        return cls.r1pwh(hash=hash, hash_name=hash_name, include_parts=cls.stores(), **kwargs)
    
    r1s = restrict_one_store # alias
    
    r1swh = restrict_one_store_with_hash # alias
    
    @classmethod
    def restrict_makers(cls, restr={}, **kwargs):
        return cls.restrict_parts(part_restr=restr, include_parts=cls.makers(), **kwargs)
        
    @classmethod
    def restrict_one_maker(cls, restr={}, **kwargs):
        return cls.r1p(part_restr=restr, include_parts=cls.makers(), **kwargs)
    
    @classmethod
    def restrict_one_maker_with_hash(cls, hash, hash_name=None, **kwargs):
        return cls.r1pwh(hash=hash, hash_name=hash_name, include_parts=cls.makers(), **kwargs)
    
    r1m = restrict_one_maker # alias 
    
    r1mwh = restrict_one_maker_with_hash # alias


class MethodGroup(MotifMaster, BaseMaster, dj.Lookup):
    @classmethod
    def _init_validation(cls, **kwargs):
        assert getattr(cls, 'hash_name', None) is not None, f'Subclasses of {cls.__base__.__name__} must implement hash_name.'

        if cls._hash_len is None:
            cls._hash_len = 32 # default hash length
        else:
            if not (isinstance(cls.hash_len, int) and (cls._hash_len > 0 and cls._hash_len <= 32)):
                raise NotImplementedError('_hash_len attribute must be an integer within range: [1, 32].')

        super()._init_validation(**kwargs)
    
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)

    @classmethod
    def is_method_group(cls):
        return issubclass(cls, MethodGroup)

class DestinationGroup(MotifMaster, BaseMaster, dj.Lookup):    
    @classmethod
    def _init_validation(cls, **kwargs):
        assert getattr(cls, 'hash_name', None) is not None, f'Subclasses of {cls.__base__.__name__} must implement hash_name.'

        if cls._hash_len is None:
            cls._hash_len = 32 # default hash length
        else:
            if not (isinstance(cls.hash_len, int) and (cls._hash_len > 0 and cls._hash_len <= 32)):
                raise NotImplementedError('_hash_len attribute must be an integer within range: [1, 32].')

        super()._init_validation(**kwargs)
        
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)

    @classmethod
    def is_destination_group(cls):
        return issubclass(cls, DestinationGroup)
    
    def update(self):
        [p().update() for p in self.parts(as_cls=True) if getattr(p, 'is_nested_destination', False)]


### NESTED
class NestedMethod(Nested, BasePart, dj.Part):
    @classmethod
    def is_nested_method(cls):
        return issubclass(cls, NestedMethod)    
        
    @classmethod
    def _init_validation(cls, **kwargs):
        if not getattr(cls, 'enable_hashing', False):
            raise NotImplementedError(f'Subclasses of {cls.__base__.__name__} must have enable_hashing set to True.')

        super()._init_validation(**kwargs)

    def __init_subclass__(cls, **kwargs):
        cls.enable_hashing = True
        cls._init_validation(**kwargs)


class NestedStore(Nested, BasePart, dj.Part):
    @classmethod
    def _init_validation(cls, **kwargs):
        super()._init_validation(**kwargs)

    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)

    @classmethod
    def is_nested_store(cls):
        return issubclass(cls, NestedStore)

    @classmethod
    def put(cls, result):
        cls.insert1(result, ignore_extra_fields=True, insert_to_master=True)


class NestedDestination(Nested, BasePart, dj.Part):
    @classmethod
    def _init_validation(cls, **kwargs):
        super()._init_validation(**kwargs)
        
    def __init_subclass__(cls, **kwargs):
        cls.enable_hashing = True
        cls.hash_group = True
        cls.hashed_attrs = 'table_id'
        cls.definition = ''.join([
        f"""
        # {cls().__class__.__name__}
        -> master
        ---
        table_id: varchar(32) # id of table in schema.tables
        """
        ])
        cls._init_validation(**kwargs)
        

    @classmethod
    def is_nested_destination(cls):
        return issubclass(cls, NestedDestination)
    
    @classmethod
    def add_destination(cls, table_ids):
        cls.insert([{'table_id': t} for t in wrap(table_ids)], ignore_extra_fields=True, skip_duplicates=True, insert_to_master=True)

    def update(self):
        if self.destinations is not None:
            for table_id in wrap(self.destinations):
                self.add_destination(table_id)

    def put(self, result, key=None):
        if key is None:
            self.goto().put(result)
        else:
            (self & key).goto().put(result)


class NestedMaker(Nested, BasePart, dj.Part, dj.Computed): 
    _upstream = None
    _method = None
    _destination = None

    @classmethod
    def is_nested_maker(cls):
        return issubclass(cls, NestedMaker)

    @classmethod
    def _init_validation(cls, **kwargs):
        # Single item
        for name in ['_method', '_destination']:
            item = getattr(cls, name, None)
            if isinstance(item, list) or isinstance(item, tuple):
                if len(item) > 1:
                    raise NotImplementedError(f'Only one {name[1:]} allowed in {name}.')
                else:
                    setattr(cls, name, item[0])

        if not getattr(cls, 'enable_hashing', False):
            raise NotImplementedError(f'Subclasses of {cls.__base__.__name__} must have enable_hashing set to True.')

        for required in ['hash_name', '_method', '_destination']:
            if getattr(cls, required, None) is None:
                raise NotImplementedError(f'Subclasses of {cls.__base__.__name__} must specify {required}')
        
        # Make sure _upstream wrapped in list or tuple
        for name in ['_upstream']:
            item = getattr(cls, name, None)
            if item is not None:
                setattr(cls, name, wrap(item))

    def __init_subclass__(cls, **kwargs):
        cls.enable_hashing = True
        cls._init_validation(**kwargs)

        for name in ['upstream', 'method', 'destination']:
            setattr(cls, name, classproperty(getattr(cls, name)))

        if getattr(cls, 'hashed_attrs', None) is None:
            cls.hashed_attrs = cls.key_source.primary_key
        if getattr(cls, '_upstream_definition', None) is None:
            cls._upstream_definition = ''.join([f'-> {u.class_name} \n' for u in cls._upstream]) if cls._upstream is not None else ''
        if getattr(cls, '_method_definition', None) is None:
            cls._method_definition = ''.join(['-> ', cls._method.class_name, '\n'])
        if getattr(cls, '_destination_definition', None) is None:
            cls._destination_definition = ''.join(['-> ', cls._destination.class_name, '\n'])
        if getattr(cls, 'additional_pk_definition', None) is None:
            cls._additional_pk_definition = ''
        if getattr(cls, 'additional_sk_definition', None) is None:
            cls._additional_sk_definition = ''

        cls.definition = ''.join([
            f"""
            # {cls().__class__.__name__}
            -> master
            """,
            cls._upstream_definition,
            cls._method_definition,
            cls._additional_pk_definition,
            """
            ---
            """,
            cls._destination_definition,
            cls._additional_sk_definition,
            """
            ts_inserted=CURRENT_TIMESTAMP: timestamp #
            """
        ])

        super()._init_validation(**kwargs)

    def upstream(cls):
        if cls._upstream is not None:
            names = []
            for up in cls._upstream:
                try:
                    names.append(up.class_name)
                except AttributeError:
                    names.append(format_table_name(split_full_table_name(up.from_clause)[1]) + up.__class__.__name__)

            return namedtuple('Upstream', *names)(*cls._upstream)

    def method(cls):
        return cls._method
    
    def destination(cls):
        return cls._destination

    @property
    def key_source(self):
        if self.upstream is not None:
            return np.product(*self.upstream) * self.method
        else:
            return self.method

    def make(self, key):
        inputs = {}
        if self.upstream is not None:
            for u in self.upstream:
                if inspect.isclass(u):
                    u = u()
                if getattr(self, 'get_upstream_keys_only', False):
                    inputs.update(**(u & key).fetch1('KEY'))
                else:
                    try:
                        inputs.update(**u.get(key))
                    except:
                        inputs.update(**(u & key).fetch1())

        if getattr(self.method, 'is_method_group', False):
            result = self.method.r1p(key).run(inputs)
        elif getattr(self.method, 'is_nested_method', False):
            result = (self.method & key).run(inputs)
        else:
            raise AttributeError(f'table type {self.method.__class__.__base__.__name__} not supported.')
            
        result.update({self.hash_name: self.hash1(key)})
        
        if inspect.isclass(self.destination):
            self.destination = self.destination()
        self.destination.update()
        dkey = self.destination.fetch1('KEY')
        if getattr(self.destination, 'is_destination_group', False):
            destination = self.destination.r1p(dkey)
        elif getattr(self.destination, 'is_nested_destination', False):
            destination = (self.destination & dkey)
        else:
            raise AttributeError(f'table type {self.destination.__class__.__base__.__name__} not a supported.')
        
        key.update(**dkey)
        destination.put(result)
        self.insert1(key)


# class NestedMethodAdapter(Nested, BasePart, dj.Part, dj.Imported):
#     _upstream = None
#     _method = None
#     _destination = None
    
#     @classmethod
#     def _init_validation(cls, **kwargs):
#         for required in ['_method']:
#             if getattr(cls, required) is None:
#                 raise NotImplementedError(f'Subclasses of NestedCompute must specify {required}')

#         for name in ['_upstream', '_destination']:
#             if getattr(cls, name) is not None:
#                 raise NotImplementedError(f'Subclasses of NestedMethodAdapter must not contain {name}.')
        
#         if isinstance(cls._method, list) or isinstance(cls._method, tuple):
#             if len(cls._method) > 1:
#                 raise NotImplementedError(f'Only one class allowed in method.')
#             else:
#                 cls._method = cls._method[0]
        
#     def __init_subclass__(cls, **kwargs):
#         cls.hashed_attrs = cls.key_source.heading.primary_key
#         cls.definition = ''.join([
#             f"""
#             # {cls().__class__.__name__}
#             -> master
#             """,
#             ''.join(['->', cls._method.class_name, '\n']),
#             """
#             ---
#             ts_inserted=CURRENT_TIMESTAMP: timestamp #
#             """
#         ])
        
#         cls._init_validation(**kwargs)

#         @classproperty
#         def method(cls):
#             return cls._method

#         @classproperty
#         def key_source(cls):
#             return cls.method
    
#     def run(self, **kwargs):
#         return self.method.r1p(self).run(**kwargs)
