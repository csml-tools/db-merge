# db-merge tool

## Installation

```
pip install git+https://github.com/csml-tools/db-merge
```

## Usage

```
db-merge INPUTS... -o OUTPUT [-c OPTIONS]
```

where

- `INPUTS` are one or more [sqlalchemy-style URLs](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls),
  like `sqlite:///file.sqlite`.  
  Slices can be manually specified with a `#` prefix: `2#sqlite:///input1.sqlite` `4#sqlite:///input2.sqlite`,  
  or will be assigned automatically, starting from 0
- `-o OUTPUT` is a [sqlalchemy-style URL](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls)
- `-c OPTIONS` is a JSON or YAML configuration file

First, run the command without options to see what tables are overlapping:

```
❯ db-merge sqlite:///scival.sqlite sqlite:///pure.sqlite -o sqlite:///merged.sqlite
Unclassified overlapping tables:
csml_record
csml_source
csml_type_database_record
csml_record_affiliation
csml_record_author
csml_record_category
csml_type_category
csml_record_ids
csml_type_record_ids
```

Then you need to assign these tables to one of the 3 categories:

- **exclude**: ignored, not included in output
- **same**: reference tables, must have the same data across all inputs
- **sliced**: have a designated `slice_column`, each input sits at its own slice

Note that if a table is marked as `sliced`, its entire subtree is considered `sliced` as well,
so the following config is enough to resolve all conflicts above:

```yaml
same:
  - csml_type_record_ids
  - csml_type_category
  - csml_type_database_record

sliced:
  - table: csml_source
    slice_column: id_slice
  - table: csml_record
    slice_column: id_slice
```

Now, repeat with the config:

```
db-merge sqlite:///scival.sqlite sqlite:///pure.sqlite -c options.yaml -o sqlite:///merged.sqlite
```
