-module(kura_timestamps_tests).
-moduledoc "Which fields the repo stamps, read through a recording driver.".

-include_lib("eunit/include/eunit.hrl").

-define(REPO, kura_timestamps_test_repo).
-define(CUSTOM, kura_test_custom_timestamps_schema).
-define(LISTED, kura_test_listed_timestamps_schema).
-define(INSERT_ONLY, kura_test_insert_stamp_schema).
-define(NONE, kura_test_no_timestamps_schema).

-define(BAD, kura_test_bad_timestamps_schema).
-define(MALFORMED, kura_test_malformed_timestamps_schema).
-define(UNSTAMPED, kura_test_unstamped_schema).

-export([log/2]).

timestamp_fields_test_() ->
    [
        ?_assertEqual(
            {[inserted_at, updated_at], [updated_at]},
            kura_schema:timestamp_fields(kura_test_schema)
        ),
        ?_assertEqual({[], []}, kura_schema:timestamp_fields(kura_test_edge_schema)),
        ?_assertEqual(
            {[created_at, modified_at], [modified_at]}, kura_schema:timestamp_fields(?LISTED)
        ),
        ?_assertEqual({[], []}, kura_schema:timestamp_fields(?UNSTAMPED))
    ].

%% 2.30 stamped inserted_at and updated_at whatever timestamps/0 said, so a
%% declaration that leaves one of them out keeps it stamped and says so.
unlisted_named_field_test_() ->
    [
        {"updated_at beside the listed fields is still stamped", fun() ->
            {Fields, Warnings} = fields_and_warnings(?CUSTOM),
            ?assertEqual(
                {[created_at, modified_at, updated_at], [modified_at, updated_at]}, Fields
            ),
            ?assertMatch(
                [#{schema := ?CUSTOM, still_stamped := [updated_at], fix := _}], Warnings
            )
        end},
        {"updated_at beside an insert-only declaration is still stamped", fun() ->
            {Fields, Warnings} = fields_and_warnings(?INSERT_ONLY),
            ?assertEqual({[inserted_at, updated_at], [updated_at]}, Fields),
            ?assertMatch([#{schema := ?INSERT_ONLY, still_stamped := [updated_at]}], Warnings)
        end},
        {"a declaration that lists every stamped field says nothing", fun() ->
            ?assertEqual(
                {{[created_at, modified_at], [modified_at]}, []}, fields_and_warnings(?LISTED)
            )
        end},
        {"the warning is logged once for a schema", fun() ->
            {_, [_]} = fields_and_warnings(?CUSTOM),
            ok = logger:add_handler(?MODULE, ?MODULE, #{config => self(), level => warning}),
            try
                _ = kura_schema:timestamp_fields(?CUSTOM),
                ?assertEqual([], warnings())
            after
                logger:remove_handler(?MODULE)
            end
        end}
    ].

%% 2.30 ignored timestamps/0, so a declaration kura cannot use stamps what
%% 2.30 stamped and says so once.
unusable_declaration_test_() ->
    [
        {"a field the schema lacks: nothing is named inserted_at, so nothing is stamped", fun() ->
            {Fields, Warnings} = fields_and_warnings(?BAD),
            ?assertEqual({[], []}, Fields),
            ?assertMatch(
                [#{schema := ?BAD, timestamps := [{created_at, utc_datetime}], fix := _}],
                Warnings
            )
        end},
        {"not a list of fields: the fields are found by name", fun() ->
            {Fields, Warnings} = fields_and_warnings(?MALFORMED),
            ?assertEqual({[inserted_at, updated_at], [updated_at]}, Fields),
            ?assertMatch(
                [#{schema := ?MALFORMED, timestamps := [inserted_at, updated_at]}], Warnings
            )
        end},
        {"an empty list on a schema 2.30 stamped: still stamped", fun() ->
            {Fields, Warnings} = fields_and_warnings(?NONE),
            ?assertEqual({[inserted_at, updated_at], [updated_at]}, Fields),
            ?assertMatch([#{schema := ?NONE, timestamps := []}], Warnings)
        end},
        {"an empty list on a schema with nothing to stamp says nothing", fun() ->
            ?assertEqual({{[], []}, []}, fields_and_warnings(?UNSTAMPED))
        end},
        {"the warning is logged once for a schema", fun() ->
            {_, [_]} = fields_and_warnings(?MALFORMED),
            ok = logger:add_handler(?MODULE, ?MODULE, #{config => self(), level => warning}),
            try
                _ = kura_schema:timestamp_fields(?MALFORMED),
                ?assertEqual([], warnings())
            after
                logger:remove_handler(?MODULE)
            end
        end}
    ].

fields_and_warnings(Schema) ->
    kura_schema:forget(Schema),
    ok = logger:add_handler(?MODULE, ?MODULE, #{config => self(), level => warning}),
    try
        Fields = kura_schema:timestamp_fields(Schema),
        {Fields, warnings()}
    after
        logger:remove_handler(?MODULE)
    end.

warnings() ->
    receive
        {logged, #{timestamps := _} = Report} -> [Report | warnings()];
        {logged, _} -> warnings()
    after 100 -> []
    end.

log(#{msg := {report, Report}}, #{config := Pid}) ->
    Pid ! {logged, Report},
    ok;
log(_Event, _Config) ->
    ok.

statement_test_() ->
    {foreach, fun() -> kura_test_recording_driver:configure(?REPO) end,
        fun(_) -> kura_test_recording_driver:unconfigure() end, [
            {"insert stamps the listed fields", fun t_insert_stamps_listed/0},
            {"update stamps the second listed field", fun t_update_stamps_second/0},
            {"an unlisted updated_at is stamped as 2.30 stamped it", fun t_unlisted_updated_at/0},
            {"a value the write sets is kept", fun t_given_value_is_kept/0},
            {"insert_all stamps the listed fields", fun t_insert_all_stamps_listed/0},
            {"an empty list keeps the stamps 2.30 wrote", fun t_empty_list_keeps_the_stamps/0},
            {"an empty list with nothing to stamp stamps nothing", fun t_nothing_to_stamp/0},
            {"a field the schema lacks does not stop a write", fun t_unknown_field_still_writes/0},
            {"a malformed declaration is stamped by name", fun t_malformed_is_stamped_by_name/0}
        ]}.

statement(Run) ->
    _ = Run(),
    [Statement] = kura_test_recording_driver:statements(),
    Statement.

t_insert_stamps_listed() ->
    {SQL, Params} = statement(fun() ->
        kura_repo_worker:insert(?REPO, kura_changeset:cast(?LISTED, #{}, #{name => ~"a"}, [name]))
    end),
    ?assertEqual([~"changed_at", ~"created_at", ~"name"], insert_columns(SQL)),
    [Created, Modified] = [P || {{_, _, _}, {_, _, _}} = P <- Params],
    ?assertEqual(Created, Modified),
    ?assertEqual([~"a"], Params -- [Created, Modified]).

t_update_stamps_second() ->
    {SQL, Params} = statement(fun() ->
        kura_repo_worker:update(
            ?REPO, kura_changeset:cast(?LISTED, #{id => 1, name => ~"a"}, #{name => ~"b"}, [name])
        )
    end),
    ?assertMatch({_, _}, binary:match(SQL, ~"\"changed_at\" = $")),
    ?assertEqual(nomatch, binary:match(SQL, [~"created_at", ~"updated_at", ~"modified_at"])),
    ?assertMatch([{{_, _, _}, {_, _, _}}], Params -- [~"b", 1]).

t_unlisted_updated_at() ->
    {Insert, InsertParams} = statement(fun() ->
        kura_repo_worker:insert(?REPO, kura_changeset:cast(?CUSTOM, #{}, #{name => ~"a"}, [name]))
    end),
    ?assertEqual([~"changed_at", ~"created_at", ~"name", ~"updated_at"], insert_columns(Insert)),
    ?assertMatch([Now, Now, Now], InsertParams -- [~"a"]),
    {Update, UpdateParams} = statement(fun() ->
        kura_repo_worker:update(
            ?REPO, kura_changeset:cast(?CUSTOM, #{id => 1, name => ~"a"}, #{name => ~"b"}, [name])
        )
    end),
    ?assertMatch({_, _}, binary:match(Update, ~"\"changed_at\" = $")),
    ?assertMatch({_, _}, binary:match(Update, ~"\"updated_at\" = $")),
    ?assertEqual(nomatch, binary:match(Update, ~"created_at")),
    ?assertMatch([{{_, _, _}, {_, _, _}} = Stamp, Stamp], UpdateParams -- [~"b", 1]).

t_given_value_is_kept() ->
    Given = {{2020, 1, 2}, {3, 4, 5}},
    {_SQL, [Created, Modified]} = statement(fun() ->
        kura_repo_worker:insert(
            ?REPO,
            kura_changeset:cast(?LISTED, #{}, #{created_at => Given}, [created_at])
        )
    end),
    ?assertEqual(Given, Created),
    ?assertNotEqual(Given, Modified).

t_insert_all_stamps_listed() ->
    {SQL, Params} = statement(fun() ->
        kura_repo_worker:insert_all(?REPO, ?LISTED, [#{name => ~"a"}, #{name => ~"b"}])
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

t_empty_list_keeps_the_stamps() ->
    {Insert, InsertParams} = statement(fun() ->
        kura_repo_worker:insert(?REPO, kura_changeset:cast(?NONE, #{}, #{name => ~"a"}, [name]))
    end),
    ?assertEqual([~"inserted_at", ~"name", ~"updated_at"], insert_columns(Insert)),
    ?assertMatch([{{_, _, _}, {_, _, _}}, {{_, _, _}, {_, _, _}}], InsertParams -- [~"a"]),
    {Update, UpdateParams} = statement(fun() ->
        kura_repo_worker:update(
            ?REPO, kura_changeset:cast(?NONE, #{id => 1, name => ~"a"}, #{name => ~"b"}, [name])
        )
    end),
    ?assertMatch({_, _}, binary:match(Update, ~"\"updated_at\" = $")),
    ?assertMatch([{{_, _, _}, {_, _, _}}], UpdateParams -- [~"b", 1]).

t_nothing_to_stamp() ->
    ?assertEqual(
        {~"INSERT INTO \"plain_items\" (\"name\") VALUES ($1) RETURNING *", [~"a"]},
        statement(fun() ->
            kura_repo_worker:insert(
                ?REPO, kura_changeset:cast(?UNSTAMPED, #{}, #{name => ~"a"}, [name])
            )
        end)
    ),
    ?assertEqual(
        {~"UPDATE \"plain_items\" SET \"name\" = $1 WHERE \"id\" = $2 RETURNING *", [~"b", 1]},
        statement(fun() ->
            kura_repo_worker:update(
                ?REPO,
                kura_changeset:cast(?UNSTAMPED, #{id => 1, name => ~"a"}, #{name => ~"b"}, [name])
            )
        end)
    ).

t_unknown_field_still_writes() ->
    {SQL, Params} = statement(fun() ->
        kura_repo_worker:insert(?REPO, kura_changeset:cast(?BAD, #{}, #{name => ~"a"}, [name]))
    end),
    ?assertEqual([~"name"], insert_columns(SQL)),
    ?assertEqual([~"a"], Params).

t_malformed_is_stamped_by_name() ->
    {SQL, _Params} = statement(fun() ->
        kura_repo_worker:insert(
            ?REPO, kura_changeset:cast(?MALFORMED, #{}, #{name => ~"a"}, [name])
        )
    end),
    ?assertEqual([~"inserted_at", ~"name", ~"updated_at"], insert_columns(SQL)).
