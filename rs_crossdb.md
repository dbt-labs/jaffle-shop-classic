the only cross-database scenario that is supported today in dbt-redshift is when:
1. a cluster is used of node_type `ra3` or `serverless`
2. a `profiles.yml` profile that specifies: `ra3_node: true`
3. a `source` table is defined in a `database` different than what's given in profile
4. all models referencing the "foreign db" source table must be materialized as a table (views are not supported)

This was the only `ra3_node` scenario that dbt-redshift has supported

there is a newer feature, still in preview, [Datashare](https://docs.aws.amazon.com/redshift/latest/dg/datashare-overview.html), that allows writes to external databases. For example, if I am logged into the `FOO` database on cluster `FIZZ`, a configuration exists such that I can create tables within database `BAR` on cluster `BUZZ` (and vise versa).

The challenges in supporting this new feature are varied:

| problem                                                                                                                          | example                                                                                                                                                                                                                                                                                              |
| -------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| dbt-redshift inheirits from dbt-postgres                                                                                         | [`redshift__create_schema()`](https://github.com/dbt-labs/dbt-redshift/blob/be1a252cd51cf4a314999935734ef6ad9c9c87b5/dbt/include/redshift/macros/adapters.sql#L98-L105) which invokes `postgres__create_schema()` which only uses two-part names, `schema.relation`, exlcuding a required `database` |
| dbt-redshift relies on postgres metadata views that don't support Datashares                                                     | [`redshift__get_columns_in_relation()`](https://github.com/dbt-labs/dbt-redshift/blob/be1a252cd51cf4a314999935734ef6ad9c9c87b5/dbt/include/redshift/macros/adapters.sql#L108-L120) queries `information_schema."columns"` but should perhaps use `SVV_ALL_COLUMNS` instead`                          |
| discrepancy across Redshift SKUs for support of Datashares                                                                       | `ra3_node` and `serverless` supports Datasharing/crossdb-writes, `dc2` clusters do not                                                                                                                                                                                                               |
| discrepancy in performance between `pg_*` metadata tables and `SVV_*` datashare-supporting Redshift system tables | `information_schema."columns"` takes a few dozen millisecionds to return all column metadata in the current database. `SVV_ALL_COLUMNS` can be more than 50X slower than this                                                                                                                        |

```[tasklist]
### Tasks
- [ ] determine current & future best practice for fetching metadata
- [ ] determine performance impact of using `SVV_*` tables for all SKUs (ra3, serverless, dc2)
- [ ] shift away from `pg_` metadata queries to those that include "external" database metadata
- [ ] override any `postgres` macros that exclude `database` from `relation` name e.g. redshift__create_schema()
- [ ] (possible) remove both `verify_database` method and `ra3_node` profile parameter
- [ ] write cross database integration tests
- [ ] add RA3 and serverless to our CI pipeline
```

### related issues

```[tasklist]
### history
- [ ] https://github.com/dbt-labs/dbt-core/issues/3179
- [ ] https://github.com/dbt-labs/dbt-core/issues/3236
- [ ] https://github.com/dbt-labs/dbt-core/issues/5297
- [ ] #94
- [ ] #179
- [ ] #217
- [ ] #281 
- [ ] #501
- [ ] #555
- [ ] #656
```


## why now?

dbt-redshift still depends on dbt-postgress, which does not support cross database queries





### relevant AWS Redshift docs
- [Cross Database Overview](https://docs.aws.amazon.com/redshift/latest/dg/cross-database-overview.html)
- [Cross Database Considerations](https://docs.aws.amazon.com/redshift/latest/dg/cross-database-overview.html)