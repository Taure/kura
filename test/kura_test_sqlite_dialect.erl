-module(kura_test_sqlite_dialect).
-moduledoc """
The kura_sqlite dialect's strategy, for tests in kura: kura_dialect_pg's
SQL with each `$N` placeholder rewritten to SQLite's `?N`.
""".

-export([to_sql/1, delete_all/1, insert_all/3]).

to_sql(Query) ->
    swapped(kura_dialect_pg:to_sql(Query)).

delete_all(Query) ->
    swapped(kura_dialect_pg:delete_all(Query)).

insert_all(Table, Fields, Rows) ->
    swapped(kura_dialect_pg:insert_all(Table, Fields, Rows)).

swapped({SQL, Params}) ->
    {binary:replace(iolist_to_binary(SQL), ~"$", ~"?", [global]), Params}.
