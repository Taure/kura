-module(kura_encryption_v2_integration_tests).
-include_lib("eunit/include/eunit.hrl").

-define(SCHEMA, kura_test_enc_bound_schema).
-define(OTHER, kura_test_enc_bound_other_schema).

integration_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"insert writes v2 and get reads it back", fun t_insert_v2/0},
            {"update writes v2", fun t_update_v2/0},
            {"update_all writes v2", fun t_update_all_v2/0},
            {"update_all passes a column that is not a field through",
                fun t_update_all_raw_column/0},
            {"insert skips a virtual field", fun t_insert_virtual_field/0},
            {"insert_all writes v2", fun t_insert_all_v2/0},
            {"an encrypted array writes v2 elements", fun t_array_v2/0},
            {"rolling: v1 rows read after the flip to v2", fun t_rolling/0},
            {"a v2 frame copied to another column raises", fun t_column_transplant/0},
            {"a v2 frame copied to another table raises", fun t_table_transplant/0},
            {"a v1 frame copied to another column still decrypts", fun t_v1_transplant/0},
            {"a v2 frame swapped between rows of a column decrypts", fun t_row_swap/0},
            {"a joined encrypted column of the same name never loads as this one",
                fun t_join_colliding_column/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(minato),
    application:ensure_all_started(kura),
    application:set_env(kura, encryption, #{
        active => 1, keys => [{1, base64:encode(crypto:strong_rand_bytes(32))}]
    }),
    kura_test_repo:start(),
    drop_tables(),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE enc_bound ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255),"
        "  ssn BYTEA,"
        "  nickname BYTEA,"
        "  tags BYTEA[],"
        "  extra TEXT"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE enc_bound_other ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255),"
        "  ssn BYTEA"
        ")",
        []
    ),
    ok.

teardown(_) ->
    drop_tables(),
    application:unset_env(kura, encryption),
    application:unset_env(kura, encryption_write_format).

drop_tables() ->
    [{ok, _} = kura_test_repo:query(["DROP TABLE IF EXISTS ", T], []) || T <- tables()],
    ok.

tables() ->
    [~"enc_bound", ~"enc_bound_other"].

write(Format) ->
    application:set_env(kura, encryption_write_format, Format).

insert(Schema, Params) ->
    CS = kura_changeset:cast(Schema, #{}, Params, maps:keys(Params)),
    {ok, Record} = kura_test_repo:insert(CS),
    Record.

raw(Table, Column, Id) ->
    {ok, [Row]} = kura_test_repo:query(
        ["SELECT ", Column, " AS v FROM ", Table, " WHERE id = $1"], [Id]
    ),
    maps:get(v, Row).

assert_v2(Frame, Plain) when byte_size(Plain) >= 8 ->
    ?assertMatch(<<2, 0, 1:16, _/binary>>, Frame),
    ?assertEqual(nomatch, binary:match(Frame, Plain)).

t_insert_v2() ->
    write(v2),
    Plain = ~"123-45-6789",
    #{id := Id} = Inserted = insert(?SCHEMA, #{name => ~"a", ssn => Plain}),
    ?assertEqual(Plain, maps:get(ssn, Inserted)),
    assert_v2(raw(~"enc_bound", ~"ssn", Id), Plain),
    {ok, Fetched} = kura_test_repo:get(?SCHEMA, Id),
    ?assertEqual(Plain, maps:get(ssn, Fetched)),
    ?assertEqual(undefined, maps:get(nickname, Fetched)).

t_update_v2() ->
    write(v2),
    #{id := Id} = Record = insert(?SCHEMA, #{name => ~"b", ssn => ~"old-ssn-value"}),
    CS = kura_changeset:cast(?SCHEMA, Record, #{nickname => ~"nickname-value"}, [nickname]),
    {ok, Updated} = kura_test_repo:update(CS),
    ?assertEqual(~"nickname-value", maps:get(nickname, Updated)),
    assert_v2(raw(~"enc_bound", ~"nickname", Id), ~"nickname-value"),
    {ok, Fetched} = kura_test_repo:get(?SCHEMA, Id),
    ?assertMatch(#{ssn := ~"old-ssn-value", nickname := ~"nickname-value"}, Fetched).

t_update_all_v2() ->
    write(v2),
    #{id := Id} = insert(?SCHEMA, #{name => ~"c", ssn => ~"before-update-value"}),
    Q = kura_query:where(kura_query:from(?SCHEMA), {id, Id}),
    {ok, 1} = kura_test_repo:update_all(Q, #{ssn => ~"after-update-value"}),
    assert_v2(raw(~"enc_bound", ~"ssn", Id), ~"after-update-value"),
    {ok, Fetched} = kura_test_repo:get(?SCHEMA, Id),
    ?assertEqual(~"after-update-value", maps:get(ssn, Fetched)).

t_update_all_raw_column() ->
    write(v2),
    #{id := Id} = insert(?SCHEMA, #{name => ~"c2", ssn => ~"virtual-ssn-value"}),
    Q = kura_query:where(kura_query:from(?SCHEMA), {id, Id}),
    {ok, 1} = kura_test_repo:update_all(Q, #{extra => ~"raw", ssn => ~"updated-ssn-value"}),
    ?assertEqual(~"raw", raw(~"enc_bound", ~"extra", Id)),
    assert_v2(raw(~"enc_bound", ~"ssn", Id), ~"updated-ssn-value").

t_insert_virtual_field() ->
    write(v2),
    #{id := Id} = insert(?SCHEMA, #{
        name => ~"v", ssn => ~"virtual-ssn-value", ssn_confirmation => ~"virtual-ssn-value"
    }),
    assert_v2(raw(~"enc_bound", ~"ssn", Id), ~"virtual-ssn-value"),
    {ok, Fetched} = kura_test_repo:get(?SCHEMA, Id),
    ?assertEqual(~"virtual-ssn-value", maps:get(ssn, Fetched)).

t_insert_all_v2() ->
    write(v2),
    {ok, 2} = kura_test_repo:insert_all(?SCHEMA, [
        #{name => ~"bulk1", ssn => ~"bulk-ssn-one"}, #{name => ~"bulk2", ssn => ~"bulk-ssn-two"}
    ]),
    Q = kura_query:where(kura_query:from(?SCHEMA), {name, in, [~"bulk1", ~"bulk2"]}),
    {ok, Rows} = kura_test_repo:all(Q),
    ?assertEqual([~"bulk-ssn-one", ~"bulk-ssn-two"], lists:sort([S || #{ssn := S} <- Rows])),
    [assert_v2(raw(~"enc_bound", ~"ssn", Id), S) || #{id := Id, ssn := S} <- Rows].

t_array_v2() ->
    write(v2),
    #{id := Id} = insert(?SCHEMA, #{name => ~"d", tags => [~"tag-value-one", ~"tag-value-two"]}),
    [
        assert_v2(F, P)
     || {F, P} <- lists:zip(raw(~"enc_bound", ~"tags", Id), [~"tag-value-one", ~"tag-value-two"])
    ],
    {ok, Fetched} = kura_test_repo:get(?SCHEMA, Id),
    ?assertEqual([~"tag-value-one", ~"tag-value-two"], maps:get(tags, Fetched)).

t_rolling() ->
    write(v1),
    #{id := Old} = Record = insert(?SCHEMA, #{name => ~"e", ssn => ~"v1-value"}),
    ?assertMatch(<<1, 1, _/binary>>, raw(~"enc_bound", ~"ssn", Old)),
    write(v2),
    #{id := New} = insert(?SCHEMA, #{name => ~"f", ssn => ~"v2-value"}),
    {ok, #{ssn := ~"v1-value"}} = kura_test_repo:get(?SCHEMA, Old),
    {ok, #{ssn := ~"v2-value"}} = kura_test_repo:get(?SCHEMA, New),
    write(v1),
    {ok, #{ssn := ~"v2-value"}} = kura_test_repo:get(?SCHEMA, New),
    write(v2),
    CS = kura_changeset:cast(?SCHEMA, Record, #{ssn => ~"rewritten"}, [ssn]),
    {ok, _} = kura_test_repo:update(CS),
    assert_v2(raw(~"enc_bound", ~"ssn", Old), ~"rewritten").

t_column_transplant() ->
    write(v2),
    #{id := Id} = insert(?SCHEMA, #{name => ~"g", ssn => ~"secret"}),
    {ok, _} = kura_test_repo:query("UPDATE enc_bound SET nickname = ssn WHERE id = $1", [Id]),
    ?assertError({kura_crypto, {bad_tag, 1}}, kura_test_repo:get(?SCHEMA, Id)).

t_table_transplant() ->
    write(v2),
    #{id := Id} = insert(?SCHEMA, #{name => ~"h", ssn => ~"secret"}),
    {ok, [#{id := OtherId}]} = kura_test_repo:query(
        "INSERT INTO enc_bound_other (name, ssn) SELECT name, ssn FROM enc_bound"
        " WHERE id = $1 RETURNING id",
        [Id]
    ),
    ?assertError({kura_crypto, {bad_tag, 1}}, kura_test_repo:get(?OTHER, OtherId)).

t_v1_transplant() ->
    write(v1),
    #{id := Id} = insert(?SCHEMA, #{name => ~"i", ssn => ~"secret"}),
    {ok, _} = kura_test_repo:query("UPDATE enc_bound SET nickname = ssn WHERE id = $1", [Id]),
    {ok, Fetched} = kura_test_repo:get(?SCHEMA, Id),
    ?assertEqual(~"secret", maps:get(nickname, Fetched)).

t_row_swap() ->
    write(v2),
    #{id := A} = insert(?SCHEMA, #{name => ~"j", ssn => ~"alice"}),
    #{id := B} = insert(?SCHEMA, #{name => ~"k", ssn => ~"bob"}),
    {ok, _} = kura_test_repo:query(
        "UPDATE enc_bound SET ssn = (SELECT ssn FROM enc_bound WHERE id = $2) WHERE id = $1",
        [A, B]
    ),
    {ok, Fetched} = kura_test_repo:get(?SCHEMA, A),
    ?assertEqual(~"bob", maps:get(ssn, Fetched)).

t_join_colliding_column() ->
    write(v2),
    Name = ~"join-collision",
    {ok, _} = kura_test_repo:query("DELETE FROM enc_bound_other", []),
    insert(?SCHEMA, #{name => Name, ssn => ~"from-enc-bound"}),
    insert(?OTHER, #{name => Name, ssn => ~"from-enc-bound-other"}),
    Q = kura_query:join(kura_query:from(?SCHEMA), inner, ?OTHER, {name, name}, o),
    try kura_test_repo:all(Q) of
        {ok, [Row]} -> ?assertEqual(~"from-enc-bound", maps:get(ssn, Row))
    catch
        error:Reason -> ?assertEqual({kura_crypto, {bad_tag, 1}}, Reason)
    end.
