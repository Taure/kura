-module(kura_type_roundtrip_tests).
-moduledoc """
Integration tests for all kura types through the full DB round-trip:
cast -> dump -> PG insert -> PG select -> load.
""".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-eqwalizer({nowarn_function, t_text_large/0}).
-eqwalizer({nowarn_function, t_insert_all_types/0}).

%%----------------------------------------------------------------------
%% Test fixture
%%----------------------------------------------------------------------

type_roundtrip_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            %% Date
            {"date insert and get", fun t_date_roundtrip/0},
            {"date update", fun t_date_update/0},
            {"date nil", fun t_date_nil/0},
            {"date where clause", fun t_date_where/0},

            %% Enum
            {"enum insert as atom", fun t_enum_insert_atom/0},
            {"enum insert as binary", fun t_enum_insert_binary/0},
            {"enum update", fun t_enum_update/0},
            {"enum where clause", fun t_enum_where/0},
            {"enum invalid value rejected", fun t_enum_invalid/0},

            %% Text
            {"text insert large value", fun t_text_large/0},
            {"text update", fun t_text_update/0},

            %% Integer arrays
            {"integer array insert and get", fun t_array_int/0},
            {"integer array update", fun t_array_int_update/0},
            {"integer array empty", fun t_array_int_empty/0},

            %% Explicit datetime
            {"datetime explicit value roundtrip", fun t_datetime_explicit/0},
            {"datetime update", fun t_datetime_update/0},

            %% JSONB nested
            {"jsonb nested structure roundtrip", fun t_jsonb_nested/0},
            {"jsonb array value", fun t_jsonb_array/0},
            {"jsonb update", fun t_jsonb_update/0},
            {"jsonb null", fun t_jsonb_null/0},

            %% Combined multi-type update
            {"update multiple types at once", fun t_multi_type_update/0},

            %% insert_all with mixed types
            {"insert_all with all field types", fun t_insert_all_types/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    application:set_env(pg_types, uuid_format, string),
    kura_test_repo:start(),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS type_items ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  bio TEXT,"
        "  birth_date DATE,"
        "  status VARCHAR(255) DEFAULT 'active',"
        "  score DOUBLE PRECISION,"
        "  count INTEGER,"
        "  active BOOLEAN DEFAULT true,"
        "  tags TEXT[],"
        "  scores INTEGER[],"
        "  metadata JSONB,"
        "  ref_id UUID,"
        "  happened_at TIMESTAMPTZ,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    ok.

teardown(_) ->
    kura_test_repo:query("DROP TABLE IF EXISTS type_items CASCADE", []),
    ok.

%%----------------------------------------------------------------------
%% Test schema
%%----------------------------------------------------------------------

%% Defined in test/kura_test_type_schema.erl

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

insert_item(Name) ->
    insert_item(Name, #{}).

insert_item(Name, Extra) ->
    Params = maps:merge(#{name => Name}, Extra),
    Fields = [name | maps:keys(Extra)],
    CS = kura_changeset:cast(kura_test_type_schema, #{}, Params, Fields),
    {ok, Record} = kura_test_repo:insert(CS),
    Record.

%%----------------------------------------------------------------------
%% Date
%%----------------------------------------------------------------------

t_date_roundtrip() ->
    Item = insert_item(~"date1", #{birth_date => {2000, 6, 15}}),
    ?assertEqual({2000, 6, 15}, maps:get(birth_date, Item)),
    {ok, Fetched} = kura_test_repo:get(kura_test_type_schema, maps:get(id, Item)),
    ?assertEqual({2000, 6, 15}, maps:get(birth_date, Fetched)).

t_date_update() ->
    Item = insert_item(~"date2", #{birth_date => {1990, 1, 1}}),
    CS = kura_changeset:cast(kura_test_type_schema, Item, #{birth_date => {2025, 12, 31}}, [
        birth_date
    ]),
    {ok, Updated} = kura_test_repo:update(CS),
    ?assertEqual({2025, 12, 31}, maps:get(birth_date, Updated)).

t_date_nil() ->
    Item = insert_item(~"date3"),
    ?assertEqual(undefined, maps:get(birth_date, Item)).

t_date_where() ->
    _I1 = insert_item(~"dw1", #{birth_date => {2000, 1, 1}}),
    I2 = insert_item(~"dw2", #{birth_date => {2020, 6, 15}}),
    Q = kura_query:where(
        kura_query:from(kura_test_type_schema),
        {birth_date, '>', {2010, 1, 1}}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    Names = [maps:get(name, R) || R <- Results],
    ?assert(lists:member(~"dw2", Names)),
    ?assertNot(lists:member(~"dw1", Names)),
    _ = I2.

%%----------------------------------------------------------------------
%% Enum
%%----------------------------------------------------------------------

t_enum_insert_atom() ->
    Item = insert_item(~"enum1", #{status => active}),
    ?assertEqual(active, maps:get(status, Item)).

t_enum_insert_binary() ->
    Item = insert_item(~"enum2", #{status => ~"inactive"}),
    ?assertEqual(inactive, maps:get(status, Item)).

t_enum_update() ->
    Item = insert_item(~"enum3", #{status => active}),
    CS = kura_changeset:cast(kura_test_type_schema, Item, #{status => banned}, [status]),
    {ok, Updated} = kura_test_repo:update(CS),
    ?assertEqual(banned, maps:get(status, Updated)),
    {ok, Fetched} = kura_test_repo:get(kura_test_type_schema, maps:get(id, Item)),
    ?assertEqual(banned, maps:get(status, Fetched)).

t_enum_where() ->
    _I1 = insert_item(~"ew1", #{status => active}),
    _I2 = insert_item(~"ew2", #{status => inactive}),
    _I3 = insert_item(~"ew3", #{status => active}),
    Q = kura_query:where(
        kura_query:from(kura_test_type_schema),
        {status, ~"active"}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    ActiveNames = [maps:get(name, R) || R <- Results],
    ?assert(lists:member(~"ew1", ActiveNames)),
    ?assert(lists:member(~"ew3", ActiveNames)),
    ?assertNot(lists:member(~"ew2", ActiveNames)).

t_enum_invalid() ->
    CS = kura_changeset:cast(
        kura_test_type_schema,
        #{},
        #{name => ~"bad_enum", status => invalid_value},
        [name, status]
    ),
    ?assertNot(CS#kura_changeset.valid),
    ?assertMatch([{status, _} | _], CS#kura_changeset.errors).

%%----------------------------------------------------------------------
%% Text
%%----------------------------------------------------------------------

t_text_large() ->
    LargeText = list_to_binary(lists:duplicate(10000, $x)),
    Item = insert_item(~"text1", #{bio => LargeText}),
    ?assertEqual(10000, byte_size(maps:get(bio, Item))),
    {ok, Fetched} = kura_test_repo:get(kura_test_type_schema, maps:get(id, Item)),
    ?assertEqual(LargeText, maps:get(bio, Fetched)).

t_text_update() ->
    Item = insert_item(~"text2", #{bio => ~"original bio"}),
    CS = kura_changeset:cast(kura_test_type_schema, Item, #{bio => ~"updated bio"}, [bio]),
    {ok, Updated} = kura_test_repo:update(CS),
    ?assertEqual(~"updated bio", maps:get(bio, Updated)).

%%----------------------------------------------------------------------
%% Integer arrays
%%----------------------------------------------------------------------

t_array_int() ->
    Item = insert_item(~"arr1", #{scores => [100, 95, 87]}),
    ?assertEqual([100, 95, 87], maps:get(scores, Item)),
    {ok, Fetched} = kura_test_repo:get(kura_test_type_schema, maps:get(id, Item)),
    ?assertEqual([100, 95, 87], maps:get(scores, Fetched)).

t_array_int_update() ->
    Item = insert_item(~"arr2", #{scores => [50, 60]}),
    CS = kura_changeset:cast(kura_test_type_schema, Item, #{scores => [90, 95, 100]}, [scores]),
    {ok, Updated} = kura_test_repo:update(CS),
    ?assertEqual([90, 95, 100], maps:get(scores, Updated)).

t_array_int_empty() ->
    Item = insert_item(~"arr3", #{scores => []}),
    ?assertEqual([], maps:get(scores, Item)).

%%----------------------------------------------------------------------
%% Explicit datetime
%%----------------------------------------------------------------------

t_datetime_explicit() ->
    DT = {{2025, 3, 21}, {14, 30, 0}},
    Item = insert_item(~"dt1", #{happened_at => DT}),
    ?assertEqual(DT, maps:get(happened_at, Item)),
    {ok, Fetched} = kura_test_repo:get(kura_test_type_schema, maps:get(id, Item)),
    ?assertEqual(DT, maps:get(happened_at, Fetched)).

t_datetime_update() ->
    DT1 = {{2025, 1, 1}, {0, 0, 0}},
    DT2 = {{2026, 12, 31}, {23, 59, 59}},
    Item = insert_item(~"dt2", #{happened_at => DT1}),
    CS = kura_changeset:cast(kura_test_type_schema, Item, #{happened_at => DT2}, [happened_at]),
    {ok, Updated} = kura_test_repo:update(CS),
    ?assertEqual(DT2, maps:get(happened_at, Updated)).

%%----------------------------------------------------------------------
%% JSONB
%%----------------------------------------------------------------------

t_jsonb_nested() ->
    Nested = #{
        ~"user" => #{~"name" => ~"Alice", ~"age" => 30},
        ~"tags" => [~"admin", ~"active"],
        ~"count" => 42
    },
    Item = insert_item(~"json1", #{metadata => Nested}),
    ?assertEqual(Nested, maps:get(metadata, Item)),
    {ok, Fetched} = kura_test_repo:get(kura_test_type_schema, maps:get(id, Item)),
    ?assertEqual(Nested, maps:get(metadata, Fetched)).

t_jsonb_array() ->
    Arr = [1, ~"two", true, null],
    Item = insert_item(~"json2", #{metadata => Arr}),
    %% pgo/json returns lists as-is
    ?assertEqual(Arr, maps:get(metadata, Item)).

t_jsonb_update() ->
    Item = insert_item(~"json3", #{metadata => #{~"a" => 1}}),
    CS = kura_changeset:cast(kura_test_type_schema, Item, #{metadata => #{~"b" => 2}}, [metadata]),
    {ok, Updated} = kura_test_repo:update(CS),
    ?assertEqual(#{~"b" => 2}, maps:get(metadata, Updated)).

t_jsonb_null() ->
    Item = insert_item(~"json4"),
    ?assertEqual(undefined, maps:get(metadata, Item)).

%%----------------------------------------------------------------------
%% Combined multi-type update
%%----------------------------------------------------------------------

t_multi_type_update() ->
    Item = insert_item(~"multi1", #{
        status => active,
        score => 1.5,
        count => 10,
        active => true,
        tags => [~"a"],
        scores => [1],
        metadata => #{~"v" => 1},
        birth_date => {2000, 1, 1}
    }),
    CS = kura_changeset:cast(
        kura_test_type_schema,
        Item,
        #{
            name => ~"multi1_updated",
            status => banned,
            score => 9.9,
            count => 99,
            active => false,
            tags => [~"x", ~"y"],
            scores => [100, 200],
            metadata => #{~"v" => 2, ~"new" => true},
            birth_date => {2025, 12, 25}
        },
        [name, status, score, count, active, tags, scores, metadata, birth_date]
    ),
    {ok, Updated} = kura_test_repo:update(CS),
    ?assertEqual(~"multi1_updated", maps:get(name, Updated)),
    ?assertEqual(banned, maps:get(status, Updated)),
    ?assert(maps:get(score, Updated) > 9.0),
    ?assertEqual(99, maps:get(count, Updated)),
    ?assertEqual(false, maps:get(active, Updated)),
    ?assertEqual([~"x", ~"y"], maps:get(tags, Updated)),
    ?assertEqual([100, 200], maps:get(scores, Updated)),
    ?assertEqual(#{~"v" => 2, ~"new" => true}, maps:get(metadata, Updated)),
    ?assertEqual({2025, 12, 25}, maps:get(birth_date, Updated)).

%%----------------------------------------------------------------------
%% insert_all with types
%%----------------------------------------------------------------------

t_insert_all_types() ->
    Entries = [
        #{
            name => ~"bulk_t1",
            status => active,
            count => 1,
            score => 1.1,
            birth_date => {2000, 1, 1},
            scores => [10, 20]
        },
        #{
            name => ~"bulk_t2",
            status => inactive,
            count => 2,
            score => 2.2,
            birth_date => {2010, 6, 15},
            scores => [30]
        },
        #{
            name => ~"bulk_t3",
            status => banned,
            count => 3,
            score => 3.3,
            birth_date => {2020, 12, 31},
            scores => []
        }
    ],
    {ok, 3} = kura_repo_worker:insert_all(kura_test_repo, kura_test_type_schema, Entries),
    Q = kura_query:where(
        kura_query:from(kura_test_type_schema),
        {name, like, ~"bulk_t%"}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    ?assertEqual(3, length(Results)),
    Sorted = lists:sort(fun(A, B) -> maps:get(name, A) < maps:get(name, B) end, Results),
    [R1, R2, R3] = Sorted,
    ?assertEqual(active, maps:get(status, R1)),
    ?assertEqual(inactive, maps:get(status, R2)),
    ?assertEqual(banned, maps:get(status, R3)),
    ?assertEqual({2000, 1, 1}, maps:get(birth_date, R1)),
    ?assertEqual([10, 20], maps:get(scores, R1)),
    ?assertEqual([], maps:get(scores, R3)).
