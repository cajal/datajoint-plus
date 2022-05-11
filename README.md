# DataJointPlus
A DataJoint extension that integrates hashes and other features. Requires DataJoint version 0.12.9.

# Usage
Minimal examples for using DataJointPlus. Documentation to be added.

## Install

```bash
pip3 install datajoint-plus
```

## Create new schema
```python
import datajoint_plus as djp

schema = djp.schema("project_schema")
```

## Add table with automatic hashing

Set flag `enable_hashing=True` in any DataJoint User class (`djp.Lookup`, `djp.Manual`, `djp.Computed`, `djp.Imported`, `djp.Part`) to enable automatic hashing.

This will require two additional properties to be defined: 

`hash_name`: (string) The attribute that will contain the hash values.
* The hash_name must be added to the definition as a `varchar` type and can be added as a `primary` or `secondary` key attribute
* The hashing mechanism is `md5` and has a maximum of `32` characters. Specify how many characters from `[1, 32]` of the hash should be stored. 
    * E.g. a `12` character hash should be added to the definition as `varchar(12)`

`hashed_attrs`: (string, or list/ tuple of strings) One or more primary and/or secondary attributes that should be hashed upon insertion.

The following lookup table named `Exclusion` when created will automatically add two entries under `reason`, `"no data"` and `"incorrect data"`, with corresponding hashes under the primary key: `exclusion_hash`.

```python
@schema
class Exclusion(djp.Lookup):
    enable_hashing = True
    hash_name = 'exclusion_hash'
    hashed_attrs = 'reason'

    definition = f"""
    # reasons for excluding imported data entries
    exclusion_hash : varchar(6) 
    ---
    reason : varchar(48) # reason for exclusion
    """

    contents = [
        {'reason': 'no data'},
        {'reason': 'incorrect data'}
    ]
```

<img src="https://github.com/cajal/datajoint-plus/blob/main/images/lookup_hash_ex.png?raw=true" alt='lookup table automatically hashes values in "reason" and places the hash under "exclusion_hash"' width="500"/>

Note: The table name is automatically added to the header next to the special character `~` that is used for parsing. In addition, the `hash_name` and `hashed_attrs` are also added to the header. This can be turned off, but is useful because it allows virtual modules to parse the header and reproduce the hashing configuration, even in the absence of the code that defined the original table. 

New entries can be added to this table with direct insertion. Direct insertions must use named datatypes. Supported types include:
* pandas dataframe 
* DataJoint expression 
* Python dictionary (or list of dictionaries for multiple rows)

E.g.

```python
Exclusion.insert1(
    {'reason': 'requires manual review'}
)
```
or
```python
Exclusion.insert(
    [
        {'reason': 'requires manual review'},
        {'reason': 'some other reason'}
    ]
)
```

## Method Tables: 
### Hashing happens in part tables, Master aggregates method hashes

This example uses the master table as an aggregator for hashes. 

Each part table represents a unique method type with a pre-defined set of parameters. Every row in the part table is one method with specific parameter values. 

If a new method type is desired with a new set of parameters, a new part table can added for it at anytime. 

When individual methods are added to part tables, they recieve a hash that is also automatically added to the master table.

For maximum flexibility, downstream tables would depend on the master table.

```python
@schema
class ImportMethod(djp.Lookup):
    hash_name = 'import_method_hash' # we define a hash_name for the master even though the hashing happens in the parts
    hash_part_table_names = True # this is already True by default and need not be included but shown for clarity. With this enabled, every method will generate a unique hash across all part tables

    definition = """
    # method for importing data
    import_method_hash : varchar(12) # method hash 
    """

    class FromCSV(djp.Part):
        enable_hashing = True # the part table does the hashing
        hash_name = 'import_method_hash'
        hashed_attrs = 'param1', 'param2' # these values generate the hash

        definition = """
        # methods for loading data from a CSV
        -> master
        ---
        param1 : int # some parameter
        param2 : varchar(48) # some other parameter
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """

        def run(self):
            return self.fetch1() 
            
    class FromAPI(djp.Part):
        enable_hashing = True
        hash_name = 'import_method_hash'
        hashed_attrs = 'param1', 'param2', 'param3'

        definition = """
        # methods for importing data using some api
        -> master
        ---
        param1 : varchar(12) # 
        param2 : Decimal(3,2) # 
        param3 : int # 
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """

        def run(self):
            return self.fetch1()
```

To insert new methods, we insert to the `Part` table directly and use `insert_to_master=True`. The order of events is:
1. The hash is generated by the part table
2. The hash is inserted to the master table
3. The hash and params are inserted to the part table. 

Importantly, these steps occur in one transaction, so if any of them fail, no insertions will occur.

Inserting some methods:

```python
ImportMethod.FromCSV.insert1({'param1': 1, 'param2': 'some parameter'}, insert_to_master=True)
ImportMethod.FromCSV.insert1({'param1': 32, 'param2': 'some other parameter'}, insert_to_master=True)
```
```python
ImportMethod.FromAPI.insert1({'param1': 'a param', 'param2': 4.2, 'param3': 38}, insert_to_master=True)
ImportMethod.FromAPI.insert1({'param1': 'another param', 'param2': 6.9, 'param3': 99}, insert_to_master=True)
```
<img src="https://github.com/cajal/datajoint-plus/blob/main/images/method_table_ex_1.png?raw=true" alt='method table after insertion' width="500"/>


Now, to get to a single method we can use the master table: `ImportMethod` and the `import_method_hash` as such:

```python
ImportMethod.restrict_one_part({'import_method_hash': '902421e75df6'})
ImportMethod.r1p({'import_method_hash': '902421e75df6'}) # alias for restrict_one_part
```
or
```python
ImportMethod.restrict_one_part_with_hash('902421e75df6')
ImportMethod.r1pwh('902421e75df6') # alias for restrict_one_part_with_hash
```

Output:

<img src="https://github.com/cajal/datajoint-plus/blob/main/images/method_table_ex_2.png?raw=true" alt='method table restricted to row with hash' width="500"/>


By default, the restricted part table object is returned so we can call the run method directly:

```python
ImportMethod.r1pwh('902421e75df6').run()
```

Here, the run method calls `fetch1` on the restricted table to ensure that only one row remains after the restriction. Then it returns the parameters.

Output: 

```python
{'import_method_hash': '902421e75df6',
 'param1': 'alt',
 'param2': 6.9,
 'param3': 99,
 'ts_inserted': datetime.datetime(2022, 5, 10, 6, 19, 38)}
```

We can also recover the original hash with the `hash1` method by rehashing the parameters:

```python
ImportMethod.FromAPI.hash1(
    { 'param1': 'alt',
     'param2': 6.9,
     'param3': 99}
)

Output:
'902421e75df6'
```

## Example to be added: Dimensionality reduction

## Example to be added: Grouping/ Splitting with hashes

## Example to be added: Multiple hashes (master table performs hashing and part table performs hashing)

## Example to be added: Informational hash (hashes in the secondary key)



# Features available with DataJointPlus
Descriptions will be expanded in the future. 

## Schema-level
* `djp.schema()` - same function as DataJoint schema
* `djp.create_djp_module()` - DataJointPlus virtual module. can be used to add DataJointPlus functionality to all DataJoint tables in an imported module that did not originally have DataJointPlus, or can create a virtual module with DataJointPlus functionality from an existing schema.
* `schema.load_dependencies()` - Loads (or reloads) DataJoint networkx graph. Runs by default when schema is instantiated for both `djp.schema` and `djp.create_djp_module`. If graph is not loaded (it's not loaded by default in DataJoint 0.12.9), then `Table.parts()` returns empty list even if it has part tables. 
* `schema.tables` - a DataJoint table created automatically for every schema (similar to `~log`) that logs all tables in the schema and keeps track if they currently exist (`schema.tables.exists`) or if they were deleted (`schema.tables.deleted`). 

## Class-level flags 
These flags must be set during table definition and are constant once a table is defined. To change these features after the table is instantiated, the best practice is to delete and remake the table. 
* `enable_hashing` - (`bool`) default `False`. If `True`, `hash_name` and `hashed_attrs` must be defined.
* `hash_name` - (`str`) The name of the primary or secondary DataJoint attribute that contains the hashes
* `hashed_attrs` - (`str` or `list/tuple` of `str`) The DataJoint primary and/or secondary key attributes that will hashed upon insertion 
* `hash_group` - (`bool`) default `False`. If `True`,  multiple rows inserted simultaneously are hashed together and given the same hash
* `hash_table_name` - (`bool`) default `False`. If `True`, all hashes made in the table will also include the name of the table
* `hash_part_table_names` -  (`bool`) default `True`. Property of a master table. If `True`, enforces that all hashes made in its part tables will always include the part table name in the hash (therefore, hashes will always be unique across parts)

## Base class methods and properties 
Methods and properties common to all DJP user classes (`djp.Lookup`, `djp.Manual`, `djp.Computed`, `djp.Imported`, `djp.Part`) after they are defined. 
* `Table.Log` - Log record manager and logger. To create log: `Table.Log('info', msg)`, To view logs: `Table.Log.head()` or `Table.Log.tail()`. Directory to save logs controlled by `djp.config['log_base_dir']` or `ENV` variable: `DJ_LOG_BASE_DIR`. Default logging level set with globally with `djp.config['loglevel']` or `ENV` variable `DJ_LOGLEVEL` or for specific tables with `Table.loglevel`
* `Table.class_name` - Name of class that generated table. Part tables formatted like `'Master.Part'`
* `Table.table_id` - Unique hash for every table based on `full_table_name`
* `Table.get_earliest_entries()` - If the `Table` has a timestamp attribute, returns the `Table` restricted to the entry (or entries) that was (were) inserted earliest
* `Table.get_latest_entries()` - If the `Table` has a timestamp attribute, returns the `Table` restricted to the entry (or entries) that was (were) inserted latest
* `Table.aggr_max()` - Given an attribute name, returns the `Table` restricted to the row with the max value of that attribute
* `Table.aggr_min()` - Given an attribute name, returns the `Table` restricted to the row with the min value of that attribute
* `Table.aggr_nunique()` - Given an attribute name, returns the number of unique values in `Table` for that attribute
* `Table.include_attrs()` - returns a proj of `Table` with only provided attributes (Not guaranteed to have unique rows)
* `Table.exclude_attrs()` - returns a proj of `Table` without the provided attributes (Not guaranteed to have unique rows)
* `Table.is_master()` - `True` if `Table` is master type
* `Table.is_part()` - `True` if `Table` is part table type
* `Table.comment` - Just the user added portion of the table header
* `Table.hash_name` - name of attribute containing hash (None if no `hash_name` was defined)
* `Table.hashed_attrs` - list of attributes that get hashed upon insertion (None if `enable_hashing` is `False`)
* `Table.hash_len` - number of characters in hash 
* `Table.hash_group` - `True` if `hash_group` is enabled for `Table`
* `Table.hash_table_name` - `True` if `hash_table_name` is enabled for `Table`

## Master Table methods and properties 
Methods and properties common to DJP master user classes (`djp.Lookup`, `djp.Manual`, `djp.Computed`, `djp.Imported`) after they are defined.
* `Table.parts()` - returns a list of part tables (option to return full table names, dj.FreeTable, or class)
* `Table.has_parts()` - `True` if master has any part tables in the graph
* `Table.number_of_parts()` - Number of part tables found in graph
* `Table.restrict_parts()` - restrict all part tables with provided restriction. 
* `Table.restrict_one_part()` or alias `Table.r1p` - enforces that only one part table can be restricted successfully and returns restricted part table
* `Table.restrict_with_hash()` - pass a hash and return `Table` restricted by the hash (`Table.hash_name` must be defined or `hash_name` can be provided.)
* `Table.restrict_part_with_hash()` - pass a hash and return a list of part tables restricted with hash. Tries to return the part table class by default. 
* `Table.restrict_one_part_with_hash()` or alias `Table.r1pwh` - Same as `restrict_part_with_hash` but errors if more than one part table contains hash. If successful returns the part table. 
* `Table.hash()` - provide one or more rows and generate the same hash that `Table` would generate if those rows were inserted
* `Table.hash1()` - same as `Table.hash()` but enforces that only one hash is returned. 
* `Table.join_parts()` - join all part tables according to specific methods
* `Table.union_parts()` - union across all part tables
* `Table.hash_part_table_names` - `True` if `hash_part_table_names` was enabled for `Table`
* `Table.part_table_names_with_hash()` - returns names of all part tables that contain an entry matching provided hash
* `Table.hashes_not_in_parts()` - returns `Table` restricted to hashes that are not found in any of its part tables

## Part Table methods and properties
Methods and properties common to DJP part table classes (`djp.Part`) after they are defined.
* `Table.class_name_valid_id` - returns `Table.class_name` with `'.'` replace with `'xx'`. Useful when using `class_name` as an identifier where `'.'` characters are not allowed.



# Special cases and considerations
Documentation of special cases and considerations

## Hashing with Decimals vs Floats
In general decimals can be hashed, however depending on your use case they can pose a problem.

Below is an example table that has one value in `hashed_attrs` called `param` that is mapped to `Decimal(4,2)` in SQL:

```python
from decimal import Decimal

@schema
class DecimalExample(djp.Lookup):
    enable_hashing = True
    hash_name = 'hash'
    hashed_attrs = 'param'
    
    definition = """
    hash : varchar(20)
    ---
    param: Decimal(4,2) # decimal type
    """
```
First point to note is that the following inserts give two distinct hashes, even though value of `param` is identical in SQL:

```python
DecimalExample.insert1({'param': 8.9})
DecimalExample.insert1({'param': Decimal(8.9)})
```

<img src="https://github.com/cajal/datajoint-plus/blob/main/images/decimal_ex.png?raw=true" alt='example with Decimal datatype ' width="350"/>

Secondly, because of how SQL outputs the Decimal, neither entry can reproduce their original hash, and instead both give another hash.

```python
DecimalExample.hash1(DecimalExample.restrict_with_hash('01a86da7f62a5f33f613'))
DecimalExample.hash1(DecimalExample.restrict_with_hash('36bf4f8d74d683f345d2'))

Output:
> 'db01069f5c8ad563c10f'
```

Therefore, if hash reproducibility is required, `float` should be considered over `Decimal`.

Example with float:

```python
@schema
class FloatExample(djp.Lookup):
    enable_hashing = True
    hash_name = 'hash'
    hashed_attrs = 'param'
    
    definition = """
    hash : varchar(20)
    ---
    param: float # float type
    """

# insert
FloatExample.insert1({'param': 8.9})
```

<img src="https://github.com/cajal/datajoint-plus/blob/main/images/float_ex.png?raw=true" alt='example with float datatype ' width="350"/>

```python
# recover hash
FloatExample.hash1(FloatExample.restrict_with_hash('01a86da7f62a5f33f613'))

Output:
> '01a86da7f62a5f33f613'
```