"""
Abstract classes for DataJointPlus
"""

from collections import Counter
import inspect
from pathlib import Path
import re
import traceback

import numpy as np
import datajoint as dj
from datajoint.table import QueryExpression, FreeTable
from .config import config

from .logging import LogFileManager, getLogger
from .enum import JoinMethod
from .errors import OverwriteError, ValidationError
from .hash import generate_hash
from .heading import parse_definition, reform_definition
from .utils import classproperty, format_rows_to_df, format_table_name, unwrap, wrap, load_dependencies
from .validation import (_is_overwrite_validated,
                         _validate_hash_name_type_and_parse_hash_len,
                         pairwise_disjoint_set_validation)

logger = getLogger(__name__)

class Base:
    _is_insert_validated = False
    _enable_table_modification = True

    # required for hashing
    enable_hashing = False
    hash_name = None
    hashed_attrs = None

    # hash params
    hash_group = False
    hash_table_name = False
    _hash_len = None

    # header params
    _add_info_to_header = True
    _add_class_name_to_header = True
    _add_hash_name_to_header = True
    _add_hashed_attrs_to_header = True
    _add_hash_params_to_header = True
    
    # logging
    loglevel = config['loglevel']

    @classmethod
    def _init_validation(cls, **kwargs):
        """
        Validation for initialization of subclasses of abstract class Base. 
        """
        for attr in ['enable_hashing', 'hash_group', 'hash_table_name', '_add_hash_name_to_header', '_add_hash_params_to_header', '_add_hashed_attrs_to_header']:
            assert isinstance(getattr(cls, attr), bool), f'"{attr}" must be boolean.'           

        for attr in ['hash_name', 'hashed_attrs']:
            assert not isinstance(getattr(cls, attr), bool), f'"{attr}" must not be boolean.'
        
        if cls.enable_hashing:
            for required in ['hash_name', 'hashed_attrs']:
                if getattr(cls, required) is None:
                    raise NotImplementedError(f'Hashing requires class to implement the property "{required}".')
        
        # ensure one attribute
        for name in ['hash_name']:
            attr = getattr(cls, name)
            if attr is not None:
                if isinstance(attr, list) or isinstance(attr, tuple):
                    if len(attr) > 1:
                        raise NotImplementedError(f'Only one attribute allowed in "{name}".')
                    else:
                        attr = attr[0]

        # ensure "hashed_attrs" wrapped in list or tuple
        if cls.hashed_attrs is not None:
            if not isinstance(cls.hashed_attrs, list) and not isinstance(cls.hashed_attrs, tuple):
                cls.hashed_attrs = [cls.hashed_attrs]
            else:
                cls.hashed_attrs = cls.hashed_attrs

        # ensure sets are disjoint
        cls._must_be_disjoint = {}
        for name in ['hash_name', 'hashed_attrs']:
            attr = getattr(cls, name)
            if attr is not None:
                if not isinstance(attr, list) and not isinstance(attr, tuple):
                    cls._must_be_disjoint[name] = set([attr])
                else:
                    cls._must_be_disjoint[name] = set(attr)
        pairwise_disjoint_set_validation(list(cls._must_be_disjoint.values()), list(cls._must_be_disjoint.keys()), error=NotImplementedError)

        # set kwarg defaults
        kwargs.setdefault('hash_part_table_names', None)

        # modify header
        hash_info_dict = dict(
            add_class_name = cls._add_class_name_to_header,
            hash_name=cls.hash_name if cls._add_hash_name_to_header else None,
            hashed_attrs=cls.hashed_attrs if cls._add_hashed_attrs_to_header else None,
            hash_group=True if cls.hash_group and cls._add_hash_params_to_header else None, # only add if set to True (default is False)
            hash_table_name=True if cls.hash_table_name and cls._add_hash_params_to_header else None, # only add if set to True (default is False)
            hash_part_table_names=False if kwargs['hash_part_table_names'] is False and cls._add_hash_params_to_header else None # only add if set to False (default is True)
        )
        if cls._add_info_to_header:
            cls._modify_header(**hash_info_dict)
    
    @classproperty
    def class_name(cls):
        return cls.__qualname__

    @classproperty
    def hash_len(cls):
        if cls.hash_name is not None and cls._hash_len is None:
            cls._hash_name_validation()
        return cls._hash_len

    @classmethod
    def _insert_validation(cls):
        """
        Validation for insertion to DataJoint tables that are subclasses of abstract class Base. 
        """
        # ensure sets are disjoint
        pairwise_disjoint_set_validation(list(cls._must_be_disjoint.values()), list(cls._must_be_disjoint.keys()), error=AttributeError)


        # ensure "index" not in attributes
        if "index" in cls.heading.names:
            raise AttributeError(f'Attributes cannot be named "index". There is a bug in this DJ version that does not handle this keyword correctly with respect to MySQL.')

        cls._is_insert_validated = True

    @classmethod
    def load_dependencies(cls, force=True, verbose=True):
        """
        Loads dependencies into DataJoint networkx graph. 
        """
        load_dependencies(cls.connection, force=force, verbose=verbose)


    @classmethod
    def add_constant_attrs_to_rows(cls, rows, constant_attrs:dict={}, overwrite_rows=False):
        """
        Adds attributes to all rows.
        Warning: rows must be able to be safely converted into a pandas dataframe.
        :param rows (pd.DataFrame, QueryExpression, list, tuple): rows to pass to DataJoint `insert`. 
        :param constant_attrs (dict): Python dictionary to add to every row in rows
        :overwrite_rows (bool): Whether to overwrite key/ values in rows. If False, conflicting keys will raise a ValidationError.
        :returns: modified rows
        """   
        assert isinstance(constant_attrs, dict), 'constant_attrs must be a dict'

        rows = format_rows_to_df(rows)

        for k, v in constant_attrs.items():
            if _is_overwrite_validated(k, rows, overwrite_rows):
                rows[k] = v

        return rows

    @classmethod
    def include_attrs(cls, *args):
        """
        Returns a projection of cls that includes only the attributes passed as args.

        Note: The projection is NOT guaranteed to have unique rows, even if it contains only primary keys. 
        """
        return cls.proj(..., **{a: '""' for a in cls.heading.names if a not in args}).proj(*[a for a in cls.heading.names if a in args])
    
    @classmethod
    def exclude_attrs(cls, *args):
        """
        Returns a projection of cls that excludes all attributes passed as args. 
        
        Note: The projection is NOT guaranteed to have unique rows, even if it contains only primary keys. 
        """
        return cls.proj(..., **{a: '""' for a in cls.heading.names if a in args}).proj(*[a for a in cls.heading.names if a not in args])
             
    @classmethod
    def hash1(cls, rows, unique=False, as_dict=False, **kwargs):
        """
        Hashes rows and requires a single hash as output.
        
        Warning: rows must be able to be safely converted into a pandas dataframe.
        kwargs: args for `add_hash_to_rows`
        
        :returns (str): hash
        """
        hashes = cls.hash(rows, unique=unique, as_dict=as_dict, **kwargs)
        assert len(hashes) == 1, 'Multiple hashes found. hash1 must return only 1 hash.'
        return unwrap(hashes)

    @classmethod
    def hash(cls, rows, unique=False, as_dict=False, **kwargs):
        """
        Hashes rows.
        Warning: rows must be able to be safely converted into a pandas dataframe.
        
        kwargs: args for `add_hash_to_rows`
        :param rows: rows containing attributes to be hashed. 
        :unique: If True, only unique hashes will be returned. If False, all hashes returned. 
        
        returns (list): list with hash(es)
        """ 
        rows = cls.add_hash_to_rows(rows, overwrite_rows=True) # perform hashing
        rows = getattr(rows, cls.hash_name) # get hash name
        rows = rows.values if not unique else rows.unique() 
        return rows.tolist() if not as_dict else [{cls.hash_name: i} for i in rows]

    @classmethod
    def restrict_with_hash(cls, hash, hash_name=None):
        """
        Returns table restricted with hash. 

        :param hash: hash to restrict with
        :param hash_name: name of attribute that contains hashes. 
            If hash_name is not None:
                Will use hash_name instead of cls.hash_name
            If hash_name is None:
                Will use cls.hash_name or raise ValidationError if cls.hash_name is None

        :returns: Table restricted with hash. 
        """
        if hash_name is None and hasattr(cls, 'hash_name'):
            hash_name = cls.hash_name
        
        if hash_name is None:
            raise ValidationError('Table does not have "hash_name" defined, provide it to restrict with hash.')
            
        return cls & {cls.hash_name: hash}

    @classmethod
    def _modify_header(cls, add_class_name=True, **kwargs):
        """
        Modifies definition header using a parseable syntax
        """
        # set defaults
        kwargs.setdefault('hash_name', None)
        kwargs.setdefault('hashed_attrs', None)
        kwargs.setdefault('hash_group', None)
        kwargs.setdefault('hash_table_name', None)
        kwargs.setdefault('hash_part_table_names', None)


        if hasattr(cls, 'definition') and isinstance(cls.definition, str):
            inds, contents, _ = parse_definition(cls.definition)
            headers = contents['headers']

            if len(headers) >= 1:
                header = headers[0]
                header = header.strip(' ').strip('#').strip(' ')
                cls.comment = header
            else:
                # create header
                header = """#"""
                cls.comment = ''

            if add_class_name:
                header = f"#~{cls.class_name} | " + header[header.find("#")+1:]

            # append hash info to header
            for attr in ['hash_name', 'hash_group', 'hash_table_name', 'hash_part_table_names']:
                if kwargs[attr] is not None:
                    header += f" | {attr}: {kwargs[attr]} " 

            if kwargs['hashed_attrs'] is not None:
                header += f" | hashed_attrs: "
                for i, h in enumerate(kwargs['hashed_attrs']):
                    header += f"{h}, " if i+1 < len(kwargs['hashed_attrs']) else f"{h} "
            
            # remove spaces from missing comment
            for n in ['||', '| |', '|  |']:
                if header.find(n):
                    header = header.replace(n, '|')

            try:
                # replace existing header with modified header
                contents['headers'][0] = header
        
            except IndexError:
                # add header
                contents['headers'].extend([header])
                
                # header should go before any dependencies or attributes
                header_ind = np.min(np.concatenate([inds['dependencies'], inds['attributes']]))
                inds['headers'].extend([header_ind])
                
                # slide index over 1 to accommodate new header
                for n in [k for k in inds.keys() if k not in ['headers']]:
                    if len(inds[n])>0:
                        inds[n][inds[n] >= header_ind] += 1
            
            # reform and set definition
            cls.definition = reform_definition(inds, contents)

    @classmethod
    def parse_hash_info_from_header(cls):
        """
        Parses hash_name and hashed_attrs from DataJoint table header and sets properties in class. 
        """
        msg = 'Could not parse % from header.'
        header = cls.heading.table_info['comment']
                
        # parse class name
        class_name_parse = re.findall('~(.*?)\|', header)
        if class_name_parse:
            class_name_matches = re.findall('\w+', unwrap(class_name_parse))
            try:
                if len(class_name_matches)==1:
                    cls.class_name = class_name_matches[0]
                elif len(class_name_matches)==2:
                    cls.class_name = '.'.join(class_name_matches)
                else:
                    raise('parsed class_name did not match expected format. ')
            except:
                logger.exception(msg, 'class_name')
        else:
            cls.class_name = ''
        
        if cls.class_name:
            # remove parsed class_name
            header = header.replace('~' + unwrap(class_name_parse) + '|', '')
        
        # parse hash attributes
        matches = re.split(r'\|', header)
        if matches:
            for match in matches:
                result = re.findall('\w+', match)
                if result:
                    parseable = ['hash_name', 'hashed_attrs', 'hash_group', 'hash_table_name', 'hash_part_table_names']
                    for attr in parseable:
                        if result[0] == attr:
                            try:
                                if attr == 'hash_name':
                                    cls.hash_name = result[1]
                                    header = header.replace(match, '') # remove match
                                if attr == 'hashed_attrs':
                                    cls.hashed_attrs = result[1:]
                                    header = header.replace(match, '')

                                if result[1] == 'True' or result[1] == 'False':
                                    setattr(cls, attr, eval(result[1]))
                                    header = header.replace(match, '')
                            except:
                                logger.exception(msg, attr)
        
        cls.comment = header.strip(' ').strip('|').strip(' ')


    @classmethod
    def add_hash_to_rows(cls, rows, overwrite_rows=False):
        """
        Adds hash to rows. 
        
        Warning: rows must be able to be safely converted into a pandas dataframe.
        :param rows (pd.DataFrame, QueryExpression, list, tuple): rows to pass to DataJoint `insert`.
        :overwrite_rows (bool): Whether to overwrite key/ values in rows. If False, conflicting keys will raise a ValidationError. 
        :returns: modified rows
        """
        assert cls.hashed_attrs is not None, 'Table must have hashed_attrs defined. Check if hashing was enabled for this table.'

        hash_table_name = True if cls.hash_table_name or (issubclass(cls, dj.Part) and hasattr(cls.master, 'hash_part_table_names') and getattr(cls.master, 'hash_part_table_names')) else False

        if hash_table_name:
            table_id = {'table_id': cls.table_id}
        else:
            table_id = None
            
        rows = format_rows_to_df(rows)

        for a in cls.hashed_attrs:
            assert a in rows.columns.values, f'hashed_attr "{a}" not in rows. Row names are: {rows.columns.values}'

        if _is_overwrite_validated(cls.hash_name, rows, overwrite_rows):
            rows_to_hash = rows[[*cls.hashed_attrs]]

            if cls.hash_group:
                rows[cls.hash_name] = generate_hash(rows_to_hash, add_constant_columns=table_id)[:cls.hash_len]

            else:
                rows[cls.hash_name] = [generate_hash([row], add_constant_columns=table_id)[:cls.hash_len] for row in rows_to_hash.to_dict(orient='records')]
                
        return rows

    @classmethod
    def _prepare_insert(cls, rows, constant_attrs, overwrite_rows=False, skip_hashing=False):
        """
        Prepares rows for insert by checking if table has been validated for insert, adds constant_attrs and performs hashing. 
        """
        
        if not cls._is_insert_validated:
            cls._insert_validation()
        
        if constant_attrs != {}:
            rows = cls.add_constant_attrs_to_rows(rows, constant_attrs, overwrite_rows)

        if cls.enable_hashing and not skip_hashing:
            try:
                rows = cls.add_hash_to_rows(rows, overwrite_rows=overwrite_rows)

            except OverwriteError as err:
                new = err.args[0]
                new += ' Or, to skip the hashing step, set skip_hashing=True.'
                raise OverwriteError(new) from None

        return rows

    def get(self, key={}, attrs={}):
        """
        Wrapper around fetch that can take a key to restrict self. 
        Results always return as a dictionary.

        :param key: (dict, QueryExpression, AndList, etc) a restriction for table
        :param attrs: (str or list/ tuple) A single attr can be provided as a str, or a list/ tuple of strings

        :returns: (dict) Dictionary containing fetch1 results
        """
        try:
            if attrs != {}:
                attrs = wrap(attrs)
                result = wrap((self & key).fetch1(*attrs))
                return {a: r for a, r in zip(attrs, result)}
            else:
                return (self & key).fetch1()
        except AttributeError as e:
            raise AttributeError(e.args[0] + f'. Did you instantiate the class?') from None

    @classmethod
    def _get_attr_name_from_type(cls, attr_type):
        """
        Gets the name(s) of the attribute of type attr_type.

        :attr_type: type of attribute to search. Corresponds to DataJoint attribute types.

        :returns: (list) list of attr_names matching attr_type
        """
        return [k for k, v in cls.heading.attributes.items() if v.type == attr_type]

    @classmethod
    def aggr_min(cls, attr_name:str):
        """
        Returns table restricted by the maximum entry of attr_name.

        :param attr_name: (str) name of attribute to aggregate over

        :returns: restricted dj_table
        """
        return cls & (dj.U(attr_name) * dj.U().aggr(cls, **{attr_name: f'min({attr_name})'}))

    @classmethod
    def aggr_max(cls, attr_name:str):
        """
        Returns table restricted by the minimum entry of attr_name.

        :param attr_name: (str) name of attribute to aggregate over

        :returns: restricted dj_table
        """
        return cls & (dj.U(attr_name) * dj.U().aggr(cls, **{attr_name: f'max({attr_name})'}))

    @classmethod
    def aggr_nunique(cls, attr_name:str):
        """
        Returns table restricted by the minimum entry of attr_name.

        :param attr_name: (str) name of attribute to aggregate over

        :returns: restricted dj_table
        """
        return dj.U().aggr(cls, **{attr_name: f'count(distinct {attr_name})'}).fetch1(attr_name)

    @classmethod
    def _timestamp_attr_validation(cls, ts_name=None):
        """
        Validates that ts_name is a timestamp attribute if provided, otherwise, checks that onle one timestamp attribute exists in the table.
        """
        if ts_name is None:
            ts_name = cls._get_attr_name_from_type('timestamp')
            if isinstance(ts_name, list) and len(ts_name) == 0:
                err = 'timestamp attribute not found.'
                logger.error(err)
                raise AttributeError(err)
            elif isinstance(ts_name, list) and len(ts_name) > 1:
                err = 'Multiple timestamp attributes found. Provide desired timestamp to ts_name'
                logger.error(err)
                raise AttributeError(err)
        else:
            if cls.heading.attributes[ts_name].type != 'timestamp':
                err = f'ts_name "{ts_name}" is not a timestamp.'
                logger.error(err)
                raise AttributeError(err)
        return unwrap(ts_name)

    @classmethod
    def get_earliest_entries(cls, ts_name=None):
        """
        Returns table restricted to the earliest entries. Requires table to have timestamp attribute.

        :param ts_name: (str) Name of timestamp attribute. 
            If None: searches heading for timestamp attribute
        
        :returns: restricted dj_table
        """
        return cls.aggr_min(cls._timestamp_attr_validation(ts_name))

    @classmethod
    def get_latest_entries(cls, ts_name=None):
        """
        Returns table restricted to the latest entries. Requires table to have timestamp attribute.

        :param ts_name: (str) Name of timestamp attribute. 
            If None: searches heading for timestamp attribute
        
        :returns: (dj_table) Restricted table
        """
        return cls.aggr_max(cls._timestamp_attr_validation(ts_name))
        
    @classproperty
    def Log(cls):
        return LogFileManager(
            name=cls.__module__ + '.' + cls.__qualname__, 
            filename=Path(cls.database).joinpath(cls.__qualname__ + '.log'), 
            config_file='filehandler_timed_rotating.yml',
            level=cls.loglevel
        )
        

class BaseMaster(Base):
    hash_part_table_names = True
    _is_hash_name_validated = False

    @classmethod
    def _init_validation(cls, **kwargs):
        """
        Validation for initialization of subclasses of abstract class BaseMaster. 
        """
        for attr in ['hash_table_name', 'hash_part_table_names']:
            assert isinstance(getattr(cls, attr), bool), f'"{attr}" must be a boolean.'

        super()._init_validation(hash_table_name=cls.hash_table_name, hash_part_table_names=cls.hash_part_table_names)

    @classmethod
    def _insert_validation(cls):
        """
        Validation for insertion into subclasses of abstract class BaseMaster. 
        """
        if cls.hash_name is not None:
            if cls.hash_name not in cls.heading.names:
                raise ValidationError(f'hash_name "{cls.hash_name}" must be present in table heading.')

            # hash_name validation
            if not cls._is_hash_name_validated:
                cls._hash_name_validation()
        
        super()._insert_validation()
    
    @classmethod
    def _hash_name_validation(cls):
        """
        Validates hash_name and sets hash_len
        """       
        cls._hash_len = _validate_hash_name_type_and_parse_hash_len(cls.hash_name, cls.heading.attributes)
        cls._is_hash_name_validated = True

    @classmethod
    def parts(cls, as_objects=False, as_cls=False, reload_dependencies=False):
        """
        Wrapper around Datajoint function `parts` that enables returning parts as part_names, objects, or classes, and enables reloading of Datajoint networkx graph dependencies.

        :param as_objects: 
            If True, returns part tables as objects
            If False, returns part table names
        :param as_cls:
            If True, returns part table classes (will override as_objects)
        :param reload_dependencies: 
            If True, will force reload Datajoint networkx graph dependencies. 

        :returns: list
        """
        cls.load_dependencies(force=reload_dependencies)

        cls_parts = [getattr(cls, d) for d in dir(cls) if inspect.isclass(getattr(cls, d)) and issubclass(getattr(cls, d), dj.Part)]
        for cls_part in [p.full_table_name for p in cls_parts]:
            if cls_part not in super().parts(cls):
                logger.warning('Part table defined in class definition not found in DataJoint graph. Reload dependencies.')

        if not as_cls:
            return super().parts(cls, as_objects=as_objects)
        else:
            return cls_parts

    @classmethod
    def number_of_parts(cls, reload_dependencies=False):
        """
        Returns the number of part tables belonging to cls. 
        """
        return len(cls.parts(reload_dependencies=reload_dependencies))

    @classmethod
    def has_parts(cls, reload_dependencies=False):
        """
        Returns True if cls has part tables. 
        """
        return cls.number_of_parts(reload_dependencies=reload_dependencies) > 0

    @classmethod
    def _format_parts(cls, parts):
        """
        Formats the part tables in arg parts. 
        """
        if not isinstance(parts, list) and not isinstance(parts, tuple):
            parts = [parts]
        
        new = []
        for part in parts:
            if inspect.isclass(part) and issubclass(part, dj.Part):
                new.append(part()) # instantiate if a class

            elif isinstance(part, dj.Part):
                new.append(part)

            elif isinstance(part, QueryExpression) and not isinstance(part, dj.Part):
                raise ValidationError(f'Arg "{part.full_table_name}" is not a valid part table.')

            else:
                raise ValidationError(f'Arg "{part}" must be a part table or a list or tuple containing one or more part tables.')

        return new

    @classmethod
    def union_parts(cls, part_restr={}, include_parts=None, exclude_parts=None, filter_out_len_zero=False, reload_dependencies=False):
        """
        Returns union of part table primary keys after optional restriction. Requires all part tables in union to have identical primary keys. 

        :params: see `restrict_parts`.

        :returns: numpy array object
        """  
        return np.sum([p.proj() for p in cls.restrict_parts(part_restr=part_restr, include_parts=include_parts, exclude_parts=exclude_parts, filter_out_len_zero=filter_out_len_zero, reload_dependencies=reload_dependencies)])

#     @classmethod
#     def keys_not_in_parts(cls, part_restr={}, include_parts=None, exclude_parts=None, master_restr={}, parts_kws={}):
#         return (cls & master_restr) - cls.union_parts(include_parts=include_parts, exclude_parts=exclude_parts, part_restr=part_restr, parts_kws=parts_kws)

    @classmethod
    def join_parts(cls, part_restr={}, join_method=None, join_with_master=False, include_parts=None, exclude_parts=None, filter_out_len_zero=False, reload_dependencies=False):
        """
        Returns join of part tables after optional restriction. 

        :params part_restr, include_parts, exclude_parts: see `restrict_parts`.
        :param join_method (str):
            - 'primary_only' - will project out secondary keys and will only join on primary keys
            - 'rename_secondaries' - will add the part table to all secondary keys
            - 'rename_collisions' - will add the part table name to secondary keys that are present in more than one part table
            - 'rename_all' - will add the part table name to all primary and secondary keys

        :param join_with_master (bool): If True, parts will be joined with cls before returning. 

        :returns: numpy array object
        """
        parts = cls.restrict_parts(part_restr=part_restr, include_parts=include_parts, exclude_parts=exclude_parts, filter_out_len_zero=filter_out_len_zero, reload_dependencies=reload_dependencies)
        
        if join_with_master:
            parts = [FreeTable(cls.connection, cls.full_table_name)] + parts

        collisions = None
        if join_method is None:
            try:
                return np.product(parts)

            except:
                traceback.print_exc()
                msg = 'Join unsuccessful. Try one of the following: join_method = '
                for i, j in enumerate(JoinMethod):
                    msg += f'"{j.value}", ' if i+1 < len(JoinMethod) else f'"{j.value}".'
                print(msg)
                return
        
        elif join_method == JoinMethod.PRIMARY.value:
            return np.product([p.proj() for p in parts])
        
        elif join_method == JoinMethod.SECONDARY.value:
            attributes_to_rename = [p.heading.secondary_attributes for p in parts]
            
        elif join_method == JoinMethod.COLLISIONS.value:
            attributes_to_rename = [p.heading.secondary_attributes for p in parts]
            collisions = [item for item, count in Counter(np.concatenate(attributes_to_rename)).items() if count > 1]
            
        elif join_method == JoinMethod.ALL.value:
            attributes_to_rename = [list(p.heading.attributes.keys()) for p in parts]

        else:
            msg = f'join_method "{join_method}" not implemented. Available methods: '
            for i, j in enumerate(JoinMethod):
                msg += f'"{j.value}", ' if i+1 < len(JoinMethod) else f'"{j.value}".'
            raise NotImplementedError(msg)

        renamed_parts = []
        for p, attrs in zip(parts, attributes_to_rename):
            if isinstance(p, dj.Part):
                name = format_table_name(p.table_name, snake_case=True, part=True).split('.')[1]
            else:
                name = format_table_name(p.table_name, snake_case=True)

            if collisions is not None:
                renamed_attribute = {name + '_' + a : a for a in attrs if a in collisions}
            else:
                renamed_attribute = {name + '_' + a : a for a in attrs}
            renamed_parts.append(p.proj(..., **renamed_attribute))
            
        return np.product(renamed_parts)
    
    @classmethod
    def restrict_parts(cls, part_restr={}, include_parts=None, exclude_parts=None, filter_out_disjoint=False, filter_out_len_zero=False, reload_dependencies=False):
        """
        Restricts part tables of cls. 

        :param part_restr: restriction to restrict part tables with.
        :param include_parts: (part table or list of part tables) part table(s) to restrict. If None, will restrict all part tables of cls.
        :param exclude_parts: (part table or list of part tables) part table(s) to exclude from restriction.
        :param filter_out_disjoint: (bool) If True, included parts must have attributes that overlap with restriction. 
            Requires restriction with named attributes (e.g. dict, QueryExpression, pandas dataframe)
        :param filter_out_len_zero: (bool) If True, included parts must have greater than zero rows after restriction is applied.
        :param reload_dependencies: (bool) reloads DataJoint graph dependencies.
        """
        assert cls.has_parts(reload_dependencies=reload_dependencies), 'No part tables found. If you are expecting part tables, try with reload_dependencies=True.'

        if include_parts is None:
            parts = cls.parts(as_cls=True)
        
        else:
            parts = cls._format_parts(include_parts)
        
        if exclude_parts is not None:
            parts = [p for p in parts if p.full_table_name not in [e.full_table_name for e in cls._format_parts(exclude_parts)]]
        
        if filter_out_disjoint:
            parts = [p & part_restr for p in parts if not set(p.heading.names).isdisjoint(format_rows_to_df(part_restr).columns)]
        else:
            parts = [p & part_restr for p in parts]

        if filter_out_len_zero:
            parts = [p for p in parts if len(p)>0]

        return parts
    
    @classmethod
    def restrict_one_part(cls, part_restr={}, include_parts=None, exclude_parts=None, filter_out_disjoint=True, filter_out_len_zero=True, reload_dependencies=False):
        """
        Calls `restrict_parts` with filter_out_disjoint=True and filter_out_len_zero=True by default. If not exactly one part table is returned, then a ValidationError will be raised.

        :params: see `restrict_parts`.

        :returns: part table after restriction.
        """
        parts = cls.restrict_parts(part_restr=part_restr, include_parts=include_parts, exclude_parts=exclude_parts, filter_out_disjoint=filter_out_disjoint, filter_out_len_zero=filter_out_len_zero, reload_dependencies=reload_dependencies)

        if len(parts) > 1:
            raise ValidationError('part_restr can restrict multiple part tables.')
        
        elif len(parts) < 1:
            raise ValidationError('part_restr can not restrict any part tables.')
        
        else:
            return unwrap(parts)

    r1p = restrict_one_part # alias for restrict_one_part

    @classmethod
    def part_table_names_with_hash(cls, hash, hash_name=None, include_parts=None, exclude_parts=None, filter_out_len_zero=True, reload_dependencies=False):
        """
        Calls `restrict_parts_with_hash` with filter_out_len_zero=True by default.

        :params: see `restrict_parts_with_hash`

        :returns: list of part table names that contain hash.
        """
        parts = cls.restrict_parts_with_hash(hash=hash, hash_name=hash_name, include_parts=include_parts, exclude_parts=exclude_parts, filter_out_len_zero=filter_out_len_zero, reload_dependencies=reload_dependencies)
        return [format_table_name(r.table_name, part=True) for r in parts]

    @classmethod
    def restrict_one_part_with_hash(cls, hash, hash_name=None, include_parts=None, exclude_parts=None, filter_out_len_zero=True, reload_dependencies=False):
        """
        Calls `restrict_parts_with_hash` with filter_out_len_zero=True by default. If not exactly one part table is returned, then a ValidationError will be raised.

        :params: see `restrict_parts_with_hash`

        :returns: part table after restriction
        """
        parts = cls.restrict_parts_with_hash(hash=hash, hash_name=hash_name, include_parts=include_parts, exclude_parts=exclude_parts, filter_out_len_zero=filter_out_len_zero, reload_dependencies=reload_dependencies)

        if len(parts) > 1:
            raise ValidationError('Hash found in multiple part tables.')
        
        elif len(parts) < 1:
            raise ValidationError('Hash not found in any part tables.')
        
        else:
            return parts[0]
    
    r1pwh = restrict_one_part_with_hash # alias for restrict_one_part_with_hash

    @classmethod
    def restrict_parts_with_hash(cls, hash, hash_name=None, include_parts=None, exclude_parts=None, filter_out_len_zero=False, reload_dependencies=False):
        """
        Checks all part tables and returns the part table that is successfully restricted by {'hash_name': hash}. 

        Note: If hash_name is not provided, cls.hash_name will be tried. 

        A successful restriction is defined by:
            - 'hash_name' is in the part table heading
            - len(part & {'hash_name': hash}) > 0 if filter_out_len_zero=True

        :param hash: hash to restrict with
        :param hash_name: name of attribute that contains hash. If hash_name is None, cls.hash_name will be used.
        :params include_parts, exclude_parts, reload_dependencies: see `restrict_parts`

        :returns: list of part tables after restriction
        """  
        if hash_name is None and hasattr(cls, 'hash_name'):
            hash_name = cls.hash_name

        if hash_name is None:
            raise ValidationError('Table does not have "hash_name" defined, provide it to restrict with hash.')
        
        parts = cls.restrict_parts(part_restr={hash_name: hash}, include_parts=include_parts, exclude_parts=exclude_parts, filter_out_len_zero=filter_out_len_zero, reload_dependencies=reload_dependencies)

        return [p for p in parts if hash_name in p.heading.names]
    
    @classmethod
    def hashes_not_in_parts(cls, hash_name=None, part_restr={}, include_parts=None, exclude_parts=None, filter_out_len_zero=False, reload_dependencies=False):
        """
        Restricts master table to any hashes not found in any of its part tables.

        :param hash_name: name of attribute that contains hash. If hash_name is None, cls.hash_name will be used.
        :params part_restr, include_parts, exclude_parts, reload_dependencies: see `restrict_parts`

        :returns: cls after restriction
        """
        if hash_name is None and hasattr(cls, 'hash_name'):
            hash_name = cls.hash_name

        if hash_name is None:
            raise ValidationError('Table does not have "hash_name" defined, provide it to restrict with hash.')

        return cls - np.sum([(dj.U(cls.hash_name) & p) for p in cls.restrict_parts(part_restr=part_restr, include_parts=include_parts, exclude_parts=exclude_parts, filter_out_len_zero=filter_out_len_zero, reload_dependencies=reload_dependencies)])

    @classmethod
    def insert(cls, rows, replace=False, skip_duplicates=False, ignore_extra_fields=False, allow_direct_insert=None, reload_dependencies=False, insert_to_parts=None, insert_to_parts_kws={}, skip_hashing=False, constant_attrs={}, overwrite_rows=False):
        """
        Insert rows to cls.

        :params rows, replace, skip_duplicates, ignore_extra_fields, allow_direct_insert: see DataJoint insert function.
        :param reload_dependencies (bool): force reload DataJoint networkx graph dependencies before insert.
        :param insert_to_parts: (part table or list of part tables) part table(s) to insert to after master table insert.
        :param insert_to_parts_kws (dict): kwargs to pass to part table insert function.
        :param skip_hashing (bool): If True, hashing will be skipped if hashing is enabled. 
        :param constant_attrs (dict): Python dictionary to add to every row of rows
        :overwrite_rows (bool): Whether to overwrite key/ values in rows. If False, conflicting keys will raise a ValidationError.
        """
        error_msg = 'Error inserting to %s'

        cls.load_dependencies(force=reload_dependencies)

        if not cls._is_insert_validated:
            cls._insert_validation()
        
        rows = cls._prepare_insert(rows, constant_attrs=constant_attrs, overwrite_rows=overwrite_rows, skip_hashing=skip_hashing)
        
        if insert_to_parts is not None:
            if not dj.conn().in_transaction:
                with dj.conn().transaction:
                    try:
                        super().insert(cls(), rows=rows, replace=replace, skip_duplicates=skip_duplicates, ignore_extra_fields=ignore_extra_fields, allow_direct_insert=allow_direct_insert)
                    except:
                        logger.error(error_msg, cls.class_name)
                        raise
                    
                    try:
                        assert cls.has_parts(), 'No part tables found. If you are expecting part tables, try with reload_dependencies=True.'
                        insert_to_parts = cls._format_parts(insert_to_parts)
                        for part in insert_to_parts:
                            part.insert(rows=rows, **{'ignore_extra_fields': True}) if insert_to_parts_kws == {} else part.insert(rows=rows, **insert_to_parts_kws)

                    except:
                        logger.error(error_msg, 'part table.')
                        raise
            else:
                try:
                    super().insert(cls(), rows=rows, replace=replace, skip_duplicates=skip_duplicates, ignore_extra_fields=ignore_extra_fields, allow_direct_insert=allow_direct_insert)
                except:
                    logger.error(error_msg, cls.class_name)
                    raise
                
                try:
                    assert cls.has_parts(), 'No part tables found. If you are expecting part tables, try with reload_dependencies=True.'
                    insert_to_parts = cls._format_parts(insert_to_parts)
                    for part in insert_to_parts:
                        part.insert(rows=rows, **{'ignore_extra_fields': True}) if insert_to_parts_kws == {} else part.insert(rows=rows, **insert_to_parts_kws)

                except:
                    logger.error(error_msg, 'part table.')
                    raise
                    
        else:
            try:
                super().insert(cls(), rows=rows, replace=replace, skip_duplicates=skip_duplicates, ignore_extra_fields=ignore_extra_fields, allow_direct_insert=allow_direct_insert)
            except:
                logger.error(error_msg, cls.class_name)
                raise

    def put(self, replace=False, skip_duplicates=False, ignore_extra_fields=True, allow_direct_insert=None, reload_dependencies=False, insert_to_parts=None, insert_to_parts_kws={}, skip_hashing=False, constant_attrs={}, overwrite_rows=False, **kwargs):
        self.insert1(kwargs, replace=replace, skip_duplicates=skip_duplicates, ignore_extra_fields=ignore_extra_fields, allow_direct_insert=allow_direct_insert, reload_dependencies=reload_dependencies, insert_to_parts=insert_to_parts, insert_to_parts_kws=insert_to_parts_kws, skip_hashing=skip_hashing, constant_attrs=constant_attrs, overwrite_rows=overwrite_rows)

        
class BasePart(Base):
    _is_hash_name_validated = False

    @classmethod
    def _init_validation(cls, **kwargs):
        """
        Validation for initialization of subclasses of abstract class BasePart. 
        """
        super()._init_validation(hash_table_name=cls.hash_table_name)
    
    @classmethod
    def _hash_name_validation(cls, source='self'):
        """
        Validates hash_name and sets hash_len
        """

        part_hash_len = None
        if cls.hash_name in cls.heading.names:
            part_hash_len = _validate_hash_name_type_and_parse_hash_len(cls.hash_name, cls.heading.attributes)

        master_hash_len = None
        if cls.hash_name in cls.master.heading.names:
            master_hash_len = _validate_hash_name_type_and_parse_hash_len(cls.hash_name, cls.master.heading.attributes)

        if part_hash_len and master_hash_len:
            assert part_hash_len == master_hash_len, f'hash_name "{cls.hash_name}" varchar length mismatch. Part table length is {part_hash_len} but master length is {master_hash_len}.'        
            cls._hash_len = part_hash_len
        
        elif part_hash_len:
            cls._hash_len = part_hash_len
        
        else: 
            cls._hash_len = master_hash_len

        cls._is_hash_name_validated = True
    
    @classproperty
    def class_name_valid_id(cls):
        return cls.class_name.replace('.', 'xx')

    @classmethod
    def _insert_validation(cls):
        """
        Validation for insertion into subclasses of abstract class BasePart. 
        """
                    
        if cls.hash_name is not None:
            if not (cls.hash_name in cls.heading.names or cls.hash_name in cls.master.heading.names):
                raise ValidationError(f'hash_name: "{cls.hash_name}" must be present in the part table or master table heading.')
            
            # hash_name validation
            if not cls._is_hash_name_validated:
                cls._hash_name_validation()

        super()._insert_validation()

    @classmethod
    def insert(cls, rows, replace=False, skip_duplicates=False, ignore_extra_fields=False, allow_direct_insert=None, reload_dependencies=False, insert_to_master=False, insert_to_master_kws={}, skip_hashing=False, constant_attrs={}, overwrite_rows=False):
        """
        Insert rows to cls.

        :params rows, replace, skip_duplicates, ignore_extra_fields, allow_direct_insert: see DataJoint insert function
        :param reload_dependencies (bool): force reload DataJoint networkx graph dependencies before insert.
        :param insert_to_master (bool): whether to insert to master table before inserting to part.
        :param insert_to_master_kws (dict): kwargs to pass to master table insert function.
        :param skip_hashing (bool): If True, hashing will be skipped if hashing is enabled. 
        :param constant_attrs (dict): Python dictionary to add to every row in rows
        :overwrite_rows (bool): Whether to overwrite key/ values in rows. If False, conflicting keys will raise a ValidationError.
        """
        error_msg = 'Error inserting to %s'

        assert isinstance(insert_to_master, bool), '"insert_to_master" must be a boolean.'
        
        cls.load_dependencies(force=reload_dependencies)
        
        rows = cls._prepare_insert(rows, constant_attrs=constant_attrs, overwrite_rows=overwrite_rows, skip_hashing=skip_hashing)

        if insert_to_master:
            if not dj.conn().in_transaction:
                with dj.conn().transaction:
                    cls.master.insert(rows=rows, **{'ignore_extra_fields': True, 'skip_duplicates': True}) if insert_to_master_kws == {} else cls.master.insert(rows=rows, **insert_to_master_kws)

                    try:
                        super().insert(cls(), rows=rows, replace=replace, skip_duplicates=skip_duplicates, ignore_extra_fields=ignore_extra_fields, allow_direct_insert=allow_direct_insert)
                    except:
                        logger.error(error_msg, cls.class_name)
                        raise
            else:
                cls.master.insert(rows=rows, **{'ignore_extra_fields': True, 'skip_duplicates': True}) if insert_to_master_kws == {} else cls.master.insert(rows=rows, **insert_to_master_kws)

                try:
                    super().insert(cls(), rows=rows, replace=replace, skip_duplicates=skip_duplicates, ignore_extra_fields=ignore_extra_fields, allow_direct_insert=allow_direct_insert)
                except:
                    logger.error(error_msg, cls.class_name)
                    raise

        else:
            try:
                super().insert(cls(), rows=rows, replace=replace, skip_duplicates=skip_duplicates, ignore_extra_fields=ignore_extra_fields, allow_direct_insert=allow_direct_insert)
            except:
                logger.error(error_msg, cls.class_name)
                raise
    
    def put(self, replace=False, skip_duplicates=False, ignore_extra_fields=True, allow_direct_insert=None, reload_dependencies=False, insert_to_master=False, insert_to_master_kws={}, skip_hashing=False, constant_attrs={}, overwrite_rows=False, **kwargs):
        self.insert1(kwargs, replace=replace, skip_duplicates=skip_duplicates, ignore_extra_fields=ignore_extra_fields, allow_direct_insert=allow_direct_insert, reload_dependencies=reload_dependencies, insert_to_master=insert_to_master, insert_to_master_kws=insert_to_master_kws, skip_hashing=skip_hashing, constant_attrs=constant_attrs, overwrite_rows=overwrite_rows)
