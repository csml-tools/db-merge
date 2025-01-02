from sqlalchemy import insert, select, bindparam, MetaData, Table, Column, Integer

metadata = MetaData()
table = Table("foo", metadata, Column("__dbmerge_slice", Integer))

str(
    insert(table).from_select(["__dbmerge_slice"], select(bindparam("__dbmerge_slice")))
)
