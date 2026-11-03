-module(kura_encryption_det_integration_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(SCHEMA, kura_test_det_schema).
-define(TELEMETRY, kura_encryption_det_integration_telemetry).

integration_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        {foreach, fun reset/0, fun(_) -> ok end, [
            {"insert, then look up by equality", fun t_lookup/0},
            {"!=, in and not_in", fun t_operators/0},
            {"get_by looks up", fun t_get_by/0},
            {"update_all and delete_all look up", fun t_bulk/0},
            {"insert_all writes canonical ciphertexts", fun t_insert_all/0},
            {"the plaintext never reaches telemetry", fun t_telemetry/0},
            {"a duplicate surfaces through unique_constraint", fun t_unique/0},
            {"casefold makes uniqueness case-insensitive", fun t_casefold_unique/0},
            {"a uuid is unique in any case", fun t_uuid_unique/0},
            {"rotation: rows under both keys are found", fun t_rotation/0},
            {"rotation: uniqueness holds within one key only", fun t_rotation_unique/0},
            {"legacy reject refuses a v1 frame", fun t_legacy_reject/0},
            {"a frame copied to another column raises", fun t_transplant/0}
        ]}
    end}.

setup() ->
    application:ensure_all_started(minato),
    application:ensure_all_started(kura),
    kura_test_repo:start(),
    {ok, _} = kura_test_repo:query("DROP TABLE IF EXISTS det_users", []),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE det_users ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255),"
        "  email BYTEA UNIQUE,"
        "  ssn BYTEA UNIQUE,"
        "  token BYTEA UNIQUE,"
        "  pin BYTEA,"
        "  note BYTEA,"
        "  secret BYTEA"
        ")",
        []
    ),
    ok.

teardown(_) ->
    {ok, _} = kura_test_repo:query("DROP TABLE IF EXISTS det_users", []),
    application:unset_env(kura, encryption).

reset() ->
    keys(1, [1]),
    {ok, _} = kura_test_repo:query("TRUNCATE det_users", []),
    ok.

keys(Active, Ids) ->
    application:set_env(kura, encryption, #{
        active => 1, deterministic_active => Active, keys => [{Id, key(Id)} || Id <- Ids]
    }).

key(Id) ->
    base64:encode(binary:copy(<<Id>>, 32)).

changeset(Params) ->
    CS = kura_changeset:cast(?SCHEMA, #{}, Params, maps:keys(Params)),
    unique(unique(unique(CS, email), ssn), token).

unique(CS, Field) ->
    kura_changeset:unique_constraint(CS, Field).

insert(Params) ->
    {ok, Record} = kura_test_repo:insert(changeset(Params)),
    Record.

all(Cond) ->
    {ok, Rows} = kura_test_repo:all(kura_query:where(kura_query:from(?SCHEMA), Cond)),
    lists:sort([Name || #{name := Name} <- Rows]).

raw(Column, Id) ->
    {ok, [Row]} = kura_test_repo:query(
        ["SELECT ", Column, " AS v FROM det_users WHERE id = $1"], [Id]
    ),
    maps:get(v, Row).

t_lookup() ->
    #{id := Id} = insert(#{name => ~"ada", ssn => ~"111-22-3333", email => ~"Ada@Example.com"}),
    _ = insert(#{name => ~"bob", ssn => ~"444-55-6666"}),
    ?assertMatch(<<2, 1, 1:16, _/binary>>, raw(~"ssn", Id)),
    ?assertEqual([~"ada"], all({ssn, ~"111-22-3333"})),
    ?assertEqual([~"ada"], all({email, ~"ADA@example.COM"})),
    ?assertEqual([], all({ssn, ~"000-00-0000"})),
    {ok, Fetched} = kura_test_repo:get(?SCHEMA, Id),
    ?assertMatch(#{ssn := ~"111-22-3333", email := ~"ada@example.com"}, Fetched).

t_operators() ->
    _ = insert(#{name => ~"a", ssn => ~"s-1", pin => 1}),
    _ = insert(#{name => ~"b", ssn => ~"s-2", pin => 2}),
    _ = insert(#{name => ~"c", ssn => ~"s-3"}),
    ?assertEqual([~"b", ~"c"], all({ssn, '!=', ~"s-1"})),
    ?assertEqual([~"a", ~"c"], all({ssn, in, [~"s-1", ~"s-3"]})),
    ?assertEqual([~"b"], all({ssn, not_in, [~"s-1", ~"s-3"]})),
    ?assertEqual([~"b", ~"c"], all({'not', {ssn, ~"s-1"}})),
    ?assertEqual([~"a"], all({'or', [{pin, 1}, {ssn, ~"nope"}]})),
    ?assertEqual([~"c"], all({pin, is_nil})).

t_get_by() ->
    _ = insert(#{name => ~"ada", ssn => ~"s-9"}),
    ?assertMatch({ok, #{name := ~"ada"}}, kura_test_repo:get_by(?SCHEMA, [{ssn, ~"s-9"}])),
    ?assertMatch({error, not_found}, kura_test_repo:get_by(?SCHEMA, [{ssn, ~"s-0"}])).

t_bulk() ->
    _ = insert(#{name => ~"a", ssn => ~"s-1"}),
    _ = insert(#{name => ~"b", ssn => ~"s-2"}),
    Q = kura_query:where(kura_query:from(?SCHEMA), {ssn, ~"s-1"}),
    ?assertEqual({ok, 1}, kura_test_repo:update_all(Q, #{name => ~"a2"})),
    ?assertEqual([~"a2"], all({ssn, ~"s-1"})),
    ?assertEqual({ok, 1}, kura_test_repo:delete_all(Q)),
    ?assertEqual([~"b"], all({ssn, '!=', ~"s-1"})).

t_insert_all() ->
    Upper = ~"0190A5B2-7C3D-7E4F-8A9B-0C1D2E3F4A5B",
    {ok, 1} = kura_test_repo:insert_all(?SCHEMA, [
        #{name => ~"bulk", email => ~"BULK@Example.com", token => Upper}
    ]),
    ?assertEqual([~"bulk"], all({email, ~"bulk@example.com"})),
    ?assertEqual([~"bulk"], all({token, string:lowercase(Upper)})).

t_telemetry() ->
    Plain = ~"telemetry-ssn-0042",
    Self = self(),
    ok = telemetry:attach(
        ?TELEMETRY,
        [kura, repo, query],
        fun(_Event, _Measurements, Metadata, _Config) -> Self ! {query, Metadata} end,
        undefined
    ),
    try
        _ = insert(#{name => ~"t", ssn => Plain}),
        ?assertEqual([~"t"], all({ssn, Plain})),
        ?assertMatch({ok, _}, kura_test_repo:get_by(?SCHEMA, [{ssn, Plain}])),
        Metadata = drain([]),
        ?assert(length(Metadata) >= 3),
        ?assertEqual(nomatch, binary:match(term_to_binary(Metadata), Plain)),
        ?assert(
            lists:any(
                fun(#{params := Params}) ->
                    lists:any(fun kura_crypto:is_deterministic_frame/1, Params)
                end,
                Metadata
            )
        )
    after
        telemetry:detach(?TELEMETRY)
    end.

drain(Acc) ->
    receive
        {query, Metadata} -> drain([Metadata | Acc])
    after 100 -> Acc
    end.

t_unique() ->
    _ = insert(#{name => ~"a", ssn => ~"dup-ssn"}),
    {error, CS} = kura_test_repo:insert(changeset(#{name => ~"b", ssn => ~"dup-ssn"})),
    ?assertMatch([{ssn, ~"has already been taken"}], kura_changeset_errors(CS)).

t_casefold_unique() ->
    _ = insert(#{name => ~"a", email => ~"Ada@Example.com"}),
    {error, CS} = kura_test_repo:insert(changeset(#{name => ~"b", email => ~"ADA@EXAMPLE.COM"})),
    ?assertMatch([{email, _}], kura_changeset_errors(CS)).

t_uuid_unique() ->
    Lower = ~"0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b",
    #{token := Lower} = insert(#{name => ~"a", token => string:uppercase(Lower)}),
    {error, CS} = kura_test_repo:insert(changeset(#{name => ~"b", token => Lower})),
    ?assertMatch([{token, _}], kura_changeset_errors(CS)).

t_rotation() ->
    _ = insert(#{name => ~"old", pin => 7}),
    keys(2, [1, 2]),
    #{id := Id} = insert(#{name => ~"new", pin => 7}),
    ?assertMatch(<<2, 1, 2:16, _/binary>>, raw(~"pin", Id)),
    ?assertEqual([~"new", ~"old"], all({pin, 7})),
    ?assertEqual([~"new", ~"old"], all({pin, in, [7, 8]})),
    ?assertEqual([], all({pin, '!=', 7})).

t_rotation_unique() ->
    _ = insert(#{name => ~"old", ssn => ~"rotated"}),
    keys(2, [1, 2]),
    _ = insert(#{name => ~"new", ssn => ~"rotated"}),
    ?assertEqual([~"new", ~"old"], all({ssn, ~"rotated"})).

t_legacy_reject() ->
    #{id := Id} = insert(#{name => ~"v1"}),
    V1 = kura_crypto:encrypt(~"legacy-ssn"),
    {ok, _} = kura_test_repo:query("UPDATE det_users SET ssn = $1 WHERE id = $2", [V1, Id]),
    ?assertError({kura_crypto, {refused_version, 1}}, kura_test_repo:get(?SCHEMA, Id)).

t_transplant() ->
    #{id := Id} = insert(#{name => ~"x", ssn => ~"same-value", pin => 5}),
    {ok, _} = kura_test_repo:query("UPDATE det_users SET email = ssn WHERE id = $1", [Id]),
    ?assertError({kura_crypto, {bad_tag, 1}}, kura_test_repo:get(?SCHEMA, Id)).

kura_changeset_errors(#kura_changeset{errors = Errors}) ->
    Errors.
