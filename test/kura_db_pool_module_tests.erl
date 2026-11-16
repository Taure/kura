-module(kura_db_pool_module_tests).
-include_lib("eunit/include/eunit.hrl").

%% Verifies that kura_db:get_pool_module/1 reads the configured pool
%% impl from a repo's config and errors when none is set. This is the
%% seam that lets a repo plug in kura_pool_ets (for tests) or any
%% future kura_pool impl without touching kura_db.

-export([
    otp_app/0,
    init/1,
    %% kura_repo behaviour callbacks (unused by these tests but required)
    start/0,
    all/1,
    get/2,
    get_by/2,
    one/1,
    insert/1,
    insert/2,
    update/1,
    delete/1,
    update_all/2,
    delete_all/1,
    insert_all/2,
    insert_all/3,
    exists/1,
    reload/2,
    transaction/1,
    multi/1,
    preload/3,
    query/2
]).

-behaviour(kura_repo).

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

errors_when_pool_module_unset_test() ->
    persistent_term:put({?MODULE, config}, #{pool => some_pool}),
    try
        ?assertError({no_pool_module_configured, _}, kura_db:get_pool_module(?MODULE))
    after
        persistent_term:erase({?MODULE, config})
    end.

reads_pool_module_from_repo_config_test() ->
    persistent_term:put({?MODULE, config}, #{
        pool => some_pool,
        pool_module => kura_pool_ets
    }),
    try
        ?assertEqual(kura_pool_ets, kura_db:get_pool_module(?MODULE))
    after
        persistent_term:erase({?MODULE, config})
    end.

errors_on_non_atom_pool_module_test() ->
    persistent_term:put({?MODULE, config}, #{
        pool => some_pool,
        pool_module => "not an atom"
    }),
    try
        ?assertError({no_pool_module_configured, _}, kura_db:get_pool_module(?MODULE))
    after
        persistent_term:erase({?MODULE, config})
    end.

%%----------------------------------------------------------------------
%% get_driver_module/1
%%----------------------------------------------------------------------

errors_when_driver_module_unset_test() ->
    persistent_term:put({?MODULE, config}, #{pool => some_pool}),
    try
        ?assertError({no_driver_module_configured, _}, kura_db:get_driver_module(?MODULE))
    after
        persistent_term:erase({?MODULE, config})
    end.

reads_driver_module_from_repo_config_test() ->
    persistent_term:put({?MODULE, config}, #{
        pool => some_pool,
        driver_module => fake_driver_mod
    }),
    try
        ?assertEqual(fake_driver_mod, kura_db:get_driver_module(?MODULE))
    after
        persistent_term:erase({?MODULE, config})
    end.

errors_on_non_atom_driver_module_test() ->
    persistent_term:put({?MODULE, config}, #{
        pool => some_pool,
        driver_module => "not an atom"
    }),
    try
        ?assertError({no_driver_module_configured, _}, kura_db:get_driver_module(?MODULE))
    after
        persistent_term:erase({?MODULE, config})
    end.

%%----------------------------------------------------------------------
%% kura_repo callbacks
%%----------------------------------------------------------------------

otp_app() -> kura.

%% Override init/1 so kura_repo:config/1 returns the persistent_term we set.
init(_DefaultConfig) ->
    case persistent_term:get({?MODULE, init_calls}, undefined) of
        undefined -> ok;
        Calls -> counters:add(Calls, 1, 1)
    end,
    case persistent_term:get({?MODULE, config}, undefined) of
        undefined -> #{};
        Config -> Config
    end.

%% Behaviour stubs - never invoked by these tests.
start() -> erlang:error(not_implemented).
all(_) -> erlang:error(not_implemented).
get(_, _) -> erlang:error(not_implemented).
get_by(_, _) -> erlang:error(not_implemented).
one(_) -> erlang:error(not_implemented).
insert(_) -> erlang:error(not_implemented).
insert(_, _) -> erlang:error(not_implemented).
update(_) -> erlang:error(not_implemented).
delete(_) -> erlang:error(not_implemented).
update_all(_, _) -> erlang:error(not_implemented).
delete_all(_) -> erlang:error(not_implemented).
insert_all(_, _) -> erlang:error(not_implemented).
insert_all(_, _, _) -> erlang:error(not_implemented).
exists(_) -> erlang:error(not_implemented).
reload(_, _) -> erlang:error(not_implemented).
transaction(_) -> erlang:error(not_implemented).
multi(_) -> erlang:error(not_implemented).
preload(_, _, _) -> erlang:error(not_implemented).
query(_, _) -> erlang:error(not_implemented).

%%----------------------------------------------------------------------
%% Loading rows
%%----------------------------------------------------------------------

with_dialect(Dialect, Run) ->
    persistent_term:put({?MODULE, config}, #{pool => some_pool, dialect => Dialect}),
    try
        Run()
    after
        persistent_term:erase({?MODULE, config})
    end.

stored_row() ->
    #{
        id => 1,
        name => ~"n",
        birth_date => ~"1990-01-02",
        status => ~"active",
        score => 2,
        active => 0,
        metadata => ~"{\"a\":1}",
        ref_id => ~"0192e4a0-7b1a-7c3e-8f00-000000000001",
        happened_at => ~"2024-01-15 10:30:00",
        inserted_at => ~"2024-01-15T10:30:00Z",
        updated_at => null
    }.

load_row_reads_a_row_as_a_backend_without_native_types_stores_it_test() ->
    with_dialect(kura_test_sqlite_dialect, fun() ->
        ?assertEqual(
            #{
                id => 1,
                name => ~"n",
                birth_date => {1990, 1, 2},
                status => active,
                score => 2.0,
                active => false,
                metadata => #{~"a" => 1},
                ref_id => ~"0192e4a0-7b1a-7c3e-8f00-000000000001",
                happened_at => {{2024, 1, 15}, {10, 30, 0}},
                inserted_at => {{2024, 1, 15}, {10, 30, 0}},
                updated_at => undefined
            },
            kura_db:load_row(?MODULE, kura_test_type_schema, stored_row())
        )
    end).

%% A dialect without load_stored/2 loads what 2.30 loaded: a value that is
%% not in the Erlang form of its type stays as the driver returned it.
load_row_keeps_a_stored_form_under_a_dialect_that_reads_none_test() ->
    Stored = stored_row(),
    Loaded = Stored#{
        status := active, score := 2.0, metadata := #{~"a" => 1}, updated_at := undefined
    },
    with_dialect(kura_dialect_pg, fun() ->
        ?assertEqual(Loaded, kura_db:load_row(?MODULE, kura_test_type_schema, Stored))
    end),
    ?assertEqual(Loaded, kura_db:load_row(kura_test_type_schema, Stored)).

load_row_keeps_what_the_dialect_cannot_read_test() ->
    Row = #{id => 1, birth_date => ~"yesterday", active => 2, tags => ~"[\"a\"]"},
    with_dialect(kura_test_sqlite_dialect, fun() ->
        ?assertEqual(Row, kura_db:load_row(?MODULE, kura_test_type_schema, Row))
    end).

%% Every field type, written with dump/2 and read back: as PostgreSQL's
%% driver returns it, and as SQLite stores it. SQLite has no array column.
every_field_type_round_trips_on_both_dialects_test_() ->
    Record = #{
        id => 7,
        name => ~"name",
        bio => ~"text",
        birth_date => {1990, 1, 2},
        status => banned,
        score => 2.5,
        count => 3,
        active => true,
        tags => [~"a", ~"b"],
        scores => [1, 2],
        metadata => #{~"a" => [1, true]},
        ref_id => ~"0192e4a0-7b1a-7c3e-8f00-000000000001",
        happened_at => {{2024, 1, 15}, {10, 30, 0}},
        inserted_at => {{2024, 1, 15}, {10, 30, 0}},
        updated_at => undefined
    },
    Types = kura_schema:field_types(kura_test_type_schema),
    Dumped = #{Field => dumped(maps:get(Field, Types), Value) || Field := Value <- Record},
    [
        ?_assertEqual(lists:sort(maps:keys(Types)), lists:sort(maps:keys(Record))),
        {"PostgreSQL",
            ?_test(
                with_dialect(kura_dialect_pg, fun() ->
                    ?assertEqual(Record, kura_db:load_row(?MODULE, kura_test_type_schema, Dumped))
                end)
            )},
        {"SQLite",
            ?_test(
                with_dialect(kura_test_sqlite_dialect, fun() ->
                    Stored =
                        #{
                            Field => as_sqlite_stores(Value)
                         || Field := Value <- maps:without([tags, scores], Dumped)
                        },
                    ?assertEqual(
                        maps:without([tags, scores], Record),
                        kura_db:load_row(?MODULE, kura_test_type_schema, Stored)
                    )
                end)
            )}
    ].

dumped(Type, Value) ->
    {ok, Dumped} = kura_types:dump(Type, Value),
    Dumped.

%% What kura_driver_sqlite binds a parameter as, and so what a column holds.
as_sqlite_stores(true) ->
    1;
as_sqlite_stores(false) ->
    0;
as_sqlite_stores({{Y, Mo, D}, {H, Mi, S}}) ->
    iolist_to_binary(
        io_lib:format("~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B", [Y, Mo, D, H, Mi, S])
    );
as_sqlite_stores({Y, Mo, D}) ->
    iolist_to_binary(io_lib:format("~4..0B-~2..0B-~2..0B", [Y, Mo, D]));
as_sqlite_stores(Value) ->
    Value.

load_row_leaves_a_row_read_by_table_name_untouched_test() ->
    Row = #{id => 1, title => ~"raw"},
    ?assertEqual(Row, kura_db:load_row(~"kura_test_schema", Row)),
    ?assertEqual(Row, kura_db:load_row(?MODULE, ~"kura_test_schema", Row)).

load_rows_loads_each_row_as_load_row_does_test() ->
    Rows = [#{id => 1, active => 1}, #{id => 2, active => 0, not_a_field => ~"kept"}],
    with_dialect(kura_test_sqlite_dialect, fun() ->
        ?assertEqual(
            [#{id => 1, active => true}, #{id => 2, active => false, not_a_field => ~"kept"}],
            kura_db:load_rows(?MODULE, kura_test_type_schema, Rows)
        ),
        ?assertEqual(
            [kura_db:load_row(?MODULE, kura_test_type_schema, Row) || Row <- Rows],
            kura_db:load_rows(?MODULE, kura_test_type_schema, Rows)
        ),
        ?assertEqual([], kura_db:load_rows(?MODULE, kura_test_type_schema, [])),
        ?assertEqual(Rows, kura_db:load_rows(?MODULE, ~"kura_test_schema", Rows))
    end).

%% The dialect is asked whether it reads stored forms once for a result
%% set, not once for each row.
load_rows_resolves_the_dialect_once_test() ->
    Rows = [#{id => N, active => 1} || N <- lists:seq(1, 50)],
    with_dialect(kura_test_sqlite_dialect, fun() ->
        Calls = counters:new(1, []),
        persistent_term:put({?MODULE, init_calls}, Calls),
        try
            _ = kura_db:load_rows(?MODULE, kura_test_type_schema, Rows),
            ?assertEqual(1, counters:get(Calls, 1))
        after
            persistent_term:erase({?MODULE, init_calls})
        end
    end).

load_row_leaves_a_row_of_a_module_that_is_no_schema_untouched_test() ->
    Row = #{id => ~"1"},
    ?assertEqual(Row, kura_db:load_row(lists, Row)),
    ?assertEqual(Row, kura_db:load_row(kura_no_such_schema_module, Row)),
    ?assertEqual([Row], kura_db:load_rows(?MODULE, kura_no_such_schema_module, [Row])).

load_row_maps_a_renamed_column_onto_its_field_test() ->
    ?assertEqual(
        #{id => 1, title => ~"t", qty => 2, note => ~"n"},
        kura_db:load_row(
            kura_test_renamed_schema,
            #{item_id => 1, item_title => ~"t", quantity => 2, note => ~"n"}
        )
    ).

%% A driver that makes no atom from a column name hands the name over as text.
load_row_maps_a_renamed_column_named_in_text_onto_its_field_test() ->
    ?assertEqual(
        #{id => 1, title => ~"t", qty => 2, note => ~"n", ~"extra" => 3},
        kura_db:load_row(
            kura_test_renamed_schema,
            #{~"item_id" => 1, ~"item_title" => ~"t", ~"quantity" => 2, note => ~"n", ~"extra" => 3}
        )
    ).
