-module(kura_timestamps_tests).
-moduledoc "Which fields the repo stamps, read through a recording driver.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(REPO, kura_timestamps_test_repo).
-define(CUSTOM, kura_test_custom_timestamps_schema).
-define(NONE, kura_test_no_timestamps_schema).

timestamp_fields_test_() ->
    [
        ?_assertEqual({inserted_at, updated_at}, kura_schema:timestamp_fields(kura_test_schema)),
        ?_assertEqual({undefined, undefined}, kura_schema:timestamp_fields(kura_test_edge_schema)),
        ?_assertEqual({created_at, modified_at}, kura_schema:timestamp_fields(?CUSTOM)),
        ?_assertEqual({undefined, undefined}, kura_schema:timestamp_fields(?NONE)),
        ?_assertError(
            {kura, {unknown_timestamp_field, kura_test_bad_timestamps_schema, created_at}},
            kura_schema:timestamp_fields(kura_test_bad_timestamps_schema)
        )
    ].

statement_test_() ->
    {foreach, fun() -> kura_test_recording_driver:configure(?REPO) end,
        fun(_) -> kura_test_recording_driver:unconfigure() end, [
            {"insert stamps the listed fields", fun t_insert_stamps_listed/0},
            {"update stamps the second listed field", fun t_update_stamps_second/0},
            {"a value the write sets is kept", fun t_given_value_is_kept/0},
            {"insert_all stamps the listed fields", fun t_insert_all_stamps_listed/0},
            {"an empty list stamps nothing", fun t_empty_list_stamps_nothing/0},
            {"a field the schema lacks is refused", fun t_unknown_field_is_refused/0}
        ]}.

statement(Run) ->
    _ = Run(),
    [Statement] = kura_test_recording_driver:statements(),
    Statement.

t_insert_stamps_listed() ->
    {SQL, Params} = statement(fun() ->
        kura_repo_worker:insert(?REPO, kura_changeset:cast(?CUSTOM, #{}, #{name => ~"a"}, [name]))
    end),
    ?assertEqual([~"changed_at", ~"created_at", ~"name"], insert_columns(SQL)),
    [Created, Modified] = [P || {{_, _, _}, {_, _, _}} = P <- Params],
    ?assertEqual(Created, Modified),
    ?assertEqual([~"a"], Params -- [Created, Modified]).

t_update_stamps_second() ->
    {SQL, Params} = statement(fun() ->
        kura_repo_worker:update(
            ?REPO, kura_changeset:cast(?CUSTOM, #{id => 1, name => ~"a"}, #{name => ~"b"}, [name])
        )
    end),
    ?assertMatch({_, _}, binary:match(SQL, ~"\"changed_at\" = $")),
    ?assertEqual(nomatch, binary:match(SQL, [~"created_at", ~"updated_at", ~"modified_at"])),
    ?assertMatch([{{_, _, _}, {_, _, _}}], Params -- [~"b", 1]).

t_given_value_is_kept() ->
    Given = {{2020, 1, 2}, {3, 4, 5}},
    {_SQL, [Created, Modified]} = statement(fun() ->
        kura_repo_worker:insert(
            ?REPO,
            kura_changeset:cast(?CUSTOM, #{}, #{created_at => Given}, [created_at])
        )
    end),
    ?assertEqual(Given, Created),
    ?assertNotEqual(Given, Modified).

t_insert_all_stamps_listed() ->
    {SQL, Params} = statement(fun() ->
        kura_repo_worker:insert_all(?REPO, ?CUSTOM, [#{name => ~"a"}, #{name => ~"b"}])
    end),
    ?assertEqual([~"changed_at", ~"created_at", ~"name"], insert_columns(SQL)),
    ?assertEqual(4, length([P || {{_, _, _}, {_, _, _}} = P <- Params])),
    ?assertEqual(6, length(Params)).

insert_columns(SQL) ->
    [_, Rest] = binary:split(SQL, ~" ("),
    [Columns, _] = binary:split(Rest, ~")"),
    lists:sort([
        binary:replace(C, ~"\"", <<>>, [global])
     || C <- binary:split(Columns, ~", ", [global])
    ]).

t_empty_list_stamps_nothing() ->
    ?assertEqual(
        {~"INSERT INTO \"unstamped_items\" (\"name\") VALUES ($1) RETURNING *", [~"a"]},
        statement(fun() ->
            kura_repo_worker:insert(
                ?REPO, kura_changeset:cast(?NONE, #{}, #{name => ~"a"}, [name])
            )
        end)
    ),
    ?assertEqual(
        {~"UPDATE \"unstamped_items\" SET \"name\" = $1 WHERE \"id\" = $2 RETURNING *", [~"b", 1]},
        statement(fun() ->
            kura_repo_worker:update(
                ?REPO, kura_changeset:cast(?NONE, #{id => 1, name => ~"a"}, #{name => ~"b"}, [name])
            )
        end)
    ).

t_unknown_field_is_refused() ->
    ?assertError(
        {kura, {unknown_timestamp_field, kura_test_bad_timestamps_schema, created_at}},
        kura_repo_worker:insert(
            ?REPO,
            kura_changeset:cast(kura_test_bad_timestamps_schema, #{}, #{name => ~"a"}, [name])
        )
    ),
    ?assertEqual([], kura_test_recording_driver:statements()).
