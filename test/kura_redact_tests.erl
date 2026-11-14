-module(kura_redact_tests).
-moduledoc "Redacted fields in telemetry, the audit log and the logging helpers.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-export([forward/4]).

-define(REPO, kura_redact_test_repo).
-define(S, kura_test_secret_schema).
-define(R, ~"[redacted]").

%%----------------------------------------------------------------------
%% Which fields
%%----------------------------------------------------------------------

redacted_fields_test_() ->
    [
        ?_assertEqual([password_hash, token], kura_schema:redacted_fields(?S)),
        {"encrypted fields are redacted without being listed",
            ?_assert(lists:member(ssn, kura_schema:redacted_fields(kura_test_enc_column_schema)))},
        ?_assertEqual([], kura_schema:redacted_fields(kura_test_edge_schema)),
        ?_assertEqual([], kura_schema:redacted_fields(not_a_schema))
    ].

%%----------------------------------------------------------------------
%% Logging helpers
%%----------------------------------------------------------------------

record_test_() ->
    Record = #{id => 1, email => ~"a@b.c", password_hash => ~"h", token => ~"t"},
    [
        ?_assertEqual(
            #{id => 1, email => ~"a@b.c", password_hash => ?R, token => ?R},
            kura_redact:record(?S, Record)
        ),
        ?_assertEqual(#{email => ~"a@b.c"}, kura_redact:record(?S, #{email => ~"a@b.c"})),
        ?_assertEqual(Record, kura_redact:record(kura_test_edge_schema, Record))
    ].

changeset_test() ->
    CS = kura_changeset:cast(
        ?S,
        #{id => 1, token => ~"old"},
        #{~"email" => ~"a@b.c", ~"token" => ~"new", password_hash => ~"h"},
        [email, token, password_hash]
    ),
    Shown = kura_redact:changeset(CS),
    ?assertEqual(#{id => 1, token => ?R}, Shown#kura_changeset.data),
    ?assertEqual(
        #{email => ~"a@b.c", token => ?R, password_hash => ?R}, Shown#kura_changeset.changes
    ),
    ?assertEqual(
        [?R, ?R],
        [V || K := V <- Shown#kura_changeset.params, K =/= ~"email", K =/= email]
    ),
    ?assertEqual(nomatch, binary:match(iolist_to_binary(io_lib:format("~p", [Shown])), ~"new")).

schemaless_changeset_is_left_alone_test() ->
    CS = kura_changeset:cast(#{token => string}, #{}, #{token => ~"t"}, [token]),
    ?assertEqual(CS, kura_redact:changeset(CS)).

%%----------------------------------------------------------------------
%% Telemetry
%%----------------------------------------------------------------------

telemetry_test_() ->
    {foreach,
        fun() ->
            {ok, _} = application:ensure_all_started(telemetry),
            kura_test_recording_driver:configure(?REPO),
            kura_query_cache:flush()
        end,
        fun(_) ->
            kura_test_recording_driver:unconfigure(),
            kura_query_cache:flush()
        end,
        [
            {"insert: only the redacted fields' values", fun t_insert/0},
            {"upsert: every value", fun t_upsert/0},
            {"update: only the redacted fields' values", fun t_update/0},
            {"delete by key: nothing to redact", fun t_delete/0},
            {"insert_all: only the redacted fields' values, in every row", fun t_insert_all/0},
            {"query filtering on a redacted field: every value", fun t_query_on_redacted/0},
            {"query filtering on other fields: as it is", fun t_query_on_others/0},
            {"subquery filtering on a redacted field: every value", fun t_subquery/0},
            {"update_all: every value when it sets or filters one", fun t_update_all/0},
            {"delete_all and aggregate", fun t_delete_all_and_aggregate/0},
            {"a schema with no redacted field is shown as it is", fun t_plain_schema/0},
            {"a failed statement's error keeps its code, not its text", fun t_error_reason/0}
        ]}.

forward(_Event, _Measurements, Metadata, #{pid := Pid}) ->
    Pid ! {telemetry, Metadata}.

%% {What telemetry showed, what the driver was given}, one pair per statement.
observed(Run) ->
    Handler = {?MODULE, make_ref()},
    ok = telemetry:attach(Handler, [kura, repo, query], fun ?MODULE:forward/4, #{pid => self()}),
    try
        Result = Run(),
        {Result, events(), [Params || {_SQL, Params} <- kura_test_recording_driver:statements()]}
    after
        telemetry:detach(Handler)
    end.

events() ->
    receive
        {telemetry, Metadata} -> [Metadata | events()]
    after 0 -> []
    end.

shown(Run) ->
    {_Result, Events, Given} = observed(Run),
    {[maps:get(params, E) || E <- Events], Given}.

by_field(Fields, Params) ->
    maps:from_list(lists:zip(Fields, Params)).

changeset(Data, Params) ->
    kura_changeset:cast(?S, Data, Params, [email, password_hash, token]).

t_insert() ->
    {[Shown], [Given]} = shown(fun() ->
        kura_repo_worker:insert(
            ?REPO, changeset(#{}, #{email => ~"a@b.c", password_hash => ~"h", token => ~"t"})
        )
    end),
    Fields = lists:sort([email, password_hash, token]),
    ?assertEqual(
        #{email => ~"a@b.c", password_hash => ?R, token => ?R},
        by_field(maps:keys(maps:from_keys(Fields, x)), Shown)
    ),
    ?assertEqual([~"a@b.c", ~"h", ~"t"], lists:sort(Given)).

t_upsert() ->
    {[Shown], [Given]} = shown(fun() ->
        kura_repo_worker:insert(
            ?REPO, changeset(#{}, #{email => ~"a@b.c", token => ~"t"}), #{
                on_conflict => {email, {replace, [token]}}
            }
        )
    end),
    ?assertEqual([?R, ?R, ?R], Shown),
    ?assertEqual([~"a@b.c", ~"t", ~"t"], lists:sort(Given)).

t_update() ->
    {[Shown], [Given]} = shown(fun() ->
        kura_repo_worker:update(?REPO, changeset(#{id => 7}, #{email => ~"n@b.c", token => ~"t2"}))
    end),
    ?assertEqual([7, ?R, ~"n@b.c"], lists:sort(Shown)),
    ?assertEqual([7, ~"n@b.c", ~"t2"], lists:sort(Given)).

t_delete() ->
    ?assertEqual(
        {[[7]], [[7]]},
        shown(fun() -> kura_repo_worker:delete(?REPO, changeset(#{id => 7}, #{})) end)
    ).

t_insert_all() ->
    {[Shown], [Given]} = shown(fun() ->
        kura_repo_worker:insert_all(?REPO, ?S, [
            #{email => ~"a@b.c", token => ~"t1"}, #{email => ~"d@e.f", token => ~"t2"}
        ])
    end),
    ?assertEqual([?R, ?R, ~"a@b.c", ~"d@e.f"], lists:sort(Shown)),
    ?assertEqual([~"a@b.c", ~"d@e.f", ~"t1", ~"t2"], lists:sort(Given)).

t_query_on_redacted() ->
    Q0 = kura_query:where(kura_query:from(?S), {email, '=', ~"a@b.c"}),
    Queries = [
        kura_query:where(Q0, {token, '=', ~"t"}),
        kura_query:where(Q0, {'or', [{id, '=', 1}, {'not', {password_hash, like, ~"h%"}}]}),
        kura_query:where(Q0, {token, in, [~"t", ~"u"]}),
        kura_query:where(Q0, {token, ~"t"})
    ],
    [
        begin
            {[Shown], [Given]} = shown(fun() -> kura_repo_worker:all(?REPO, Q) end),
            ?assertEqual([?R || _ <- Given], Shown),
            ?assert(lists:member(~"a@b.c", Given))
        end
     || Q <- Queries
    ],
    {[ByKey], [[~"t"]]} = shown(fun() -> kura_repo_worker:get_by(?REPO, ?S, [{token, ~"t"}]) end),
    ?assertEqual([?R], ByKey).

t_query_on_others() ->
    Q = kura_query:where(kura_query:from(?S), {email, '=', ~"a@b.c"}),
    ?assertEqual(
        {[[~"a@b.c"]], [[~"a@b.c"]]}, shown(fun() -> kura_repo_worker:all(?REPO, Q) end)
    ),
    ?assertEqual({[[7]], [[7]]}, shown(fun() -> kura_repo_worker:get(?REPO, ?S, 7) end)).

t_subquery() ->
    Sub = kura_query:select(kura_query:where(kura_query:from(?S), {token, '=', ~"t"}), [id]),
    Q = kura_query:where(kura_query:from(?S), {id, in, {subquery, Sub}}),
    ?assertEqual({[[?R]], [[~"t"]]}, shown(fun() -> kura_repo_worker:all(?REPO, Q) end)),
    Union = kura_query:union(kura_query:from(kura_test_edge_schema), Sub),
    ?assertEqual({[[?R]], [[~"t"]]}, shown(fun() -> kura_repo_worker:all(?REPO, Union) end)).

t_update_all() ->
    ByEmail = kura_query:where(kura_query:from(?S), {email, '=', ~"a@b.c"}),
    ?assertEqual(
        {[[?R, ?R]], [[~"t", ~"a@b.c"]]},
        shown(fun() -> kura_repo_worker:update_all(?REPO, ByEmail, #{token => ~"t"}) end)
    ),
    ByToken = kura_query:where(kura_query:from(?S), {token, '=', ~"t"}),
    ?assertEqual(
        {[[?R, ?R]], [[~"n@b.c", ~"t"]]},
        shown(fun() -> kura_repo_worker:update_all(?REPO, ByToken, #{email => ~"n@b.c"}) end)
    ),
    ?assertEqual(
        {[[~"n@b.c", ~"a@b.c"]], [[~"n@b.c", ~"a@b.c"]]},
        shown(fun() -> kura_repo_worker:update_all(?REPO, ByEmail, #{email => ~"n@b.c"}) end)
    ).

t_delete_all_and_aggregate() ->
    ByToken = kura_query:where(kura_query:from(?S), {token, '=', ~"t"}),
    ?assertEqual(
        {[[?R]], [[~"t"]]}, shown(fun() -> kura_repo_worker:delete_all(?REPO, ByToken) end)
    ),
    kura_test_recording_driver:reply_with([
        #{command => select, rows => [#{count => 0}], num_rows => 1}
    ]),
    ?assertEqual(
        {[[?R]], [[~"t"]]}, shown(fun() -> kura_repo_worker:count(?REPO, ByToken) end)
    ).

t_plain_schema() ->
    Q = kura_query:where(kura_query:from(kura_test_edge_schema), {name, '=', ~"token"}),
    ?assertEqual(
        {[[~"token"]], [[~"token"]]}, shown(fun() -> kura_repo_worker:all(?REPO, Q) end)
    ).

t_error_reason() ->
    Error = #{
        code => ~"23505",
        constraint => ~"accounts_token_key",
        message => ~"duplicate key value violates unique constraint",
        detail => ~"Key (token)=(t) already exists."
    },
    kura_test_recording_driver:reply_with([{error, Error}, {error, Error}]),
    {Result, [Event], _Given} = observed(fun() ->
        kura_repo_worker:insert(?REPO, changeset(#{}, #{email => ~"a@b.c", token => ~"t"}))
    end),
    ?assertMatch({error, #kura_changeset{errors = [{_, ~"has already been taken"}]}}, Result),
    ?assertEqual(
        #{code => ~"23505", constraint => ~"accounts_token_key"}, maps:get(error_reason, Event)
    ),
    {_, [NoSecret], _} = observed(fun() ->
        kura_repo_worker:insert(?REPO, changeset(#{}, #{email => ~"a@b.c"}))
    end),
    ?assertEqual(
        #{code => ~"23505", constraint => ~"accounts_token_key"},
        maps:get(error_reason, NoSecret)
    ),
    kura_test_recording_driver:reply_with([{error, Error}]),
    {_, [Plain], _} = observed(fun() ->
        kura_repo_worker:insert(
            ?REPO, kura_changeset:cast(kura_test_edge_schema, #{}, #{name => ~"n"}, [name])
        )
    end),
    ?assertEqual(Error, maps:get(error_reason, Plain)).

error_reason_test_() ->
    [
        ?_assertEqual(timeout, kura_redact:error_reason(timeout)),
        ?_assertEqual(
            {pgsql_error, #{code => ~"22P02"}},
            kura_redact:error_reason(
                {pgsql_error, #{code => ~"22P02", message => ~"invalid input syntax: \"t\""}}
            )
        ),
        ?_assertEqual(redacted, kura_redact:error_reason({badarg, ~"t"})),
        ?_assertEqual(redacted, kura_redact:error_reason(~"t"))
    ].

%%----------------------------------------------------------------------
%% Audit log
%%----------------------------------------------------------------------

audit_test() ->
    Record = #{id => 1, email => ~"a@b.c", password_hash => ~"h", token => ~"t"},
    ?assertEqual(
        {undefined, #{id => 1, email => ~"a@b.c", password_hash => ?R, token => ?R}, undefined},
        kura_audit:build_audit_data(?S, insert, Record)
    ).
