-module(kura_test_cold_dialect).

-export([to_sql/1, convert_values/1]).

to_sql(Query) ->
    kura_dialect_pg:to_sql(Query).

convert_values(Query) ->
    kura_dialect_pg:convert_values(Query).
