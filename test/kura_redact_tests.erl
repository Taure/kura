-module(kura_redact_tests).
-moduledoc "Redacted fields in telemetry, the audit log and the logging helpers.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-export([forward/4]).

-define(REPO, kura_redact_test_repo).
-define(S, kura_test_secret_schema).
-define(OWNER, kura_test_secret_owner_schema).
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

changeset_errors_that_quote_a_redacted_value_test() ->
    Quoting = ~"invalid input syntax for type uuid: \"tok-SECRET\"",
    CS0 = kura_changeset:cast(
        ?S, #{id => 1, password_hash => ~"old-HASH"}, #{token => ~"tok-SECRET"}, [token]
    ),
    CS = CS0#kura_changeset{
        errors = [
            {base, Quoting},
            {email, ~"has invalid format"},
            {token, ~"is too short"},
            {base, ~"was old-HASH before"}
        ]
    },
    Shown = kura_redact:changeset(CS),
    ?assertEqual(
        [{base, ?R}, {email, ~"has invalid format"}, {token, ~"is too short"}, {base, ?R}],
        Shown#kura_changeset.errors
    ),
    Printed = iolist_to_binary(io_lib:format("~p", [Shown])),
    ?assertEqual(nomatch, binary:match(Printed, [~"tok-SECRET", ~"old-HASH"])).

%% A message can quote what a key of the params that names no field
%% holds, at any depth of an association's params.
changeset_errors_that_quote_a_hidden_param_test_() ->
    Errors = [
        {token, ~"tok-OTHER does not match"},
        {base, ~"member-OTHER is taken"},
        {email, ~"has invalid format"}
    ],
    Shown = [{token, ?R}, {base, ?R}, {email, ~"has invalid format"}],
    [
        {"under a key of a schema with redacted fields",
            ?_assertEqual(
                Shown,
                (kura_redact:changeset((secret_changeset())#kura_changeset{
                    params = #{
                        ~"token_confirmation" => ~"tok-OTHER", ~"usrname" => ~"member-OTHER"
                    },
                    errors = Errors
                }))#kura_changeset.errors
            )},
        {"under a key of an association's params",
            ?_assertEqual(
                Shown,
                (kura_redact:changeset(
                    (kura_changeset:cast(?OWNER, #{}, #{}, []))#kura_changeset{
                        params = #{
                            ~"accounts" => [#{~"token_confirmation" => ~"tok-OTHER"}],
                            ~"acounts" => [#{~"email" => ~"member-OTHER"}]
                        },
                        errors = Errors
                    }
                ))#kura_changeset.errors
            )},
        {"a value shown in the params is no reason to hide a message",
            ?_assertEqual(
                [{email, ~"a@b.c is taken"}],
                (kura_redact:changeset((secret_changeset())#kura_changeset{
                    params = #{~"email" => ~"a@b.c"},
                    errors = [{email, ~"a@b.c is taken"}]
                }))#kura_changeset.errors
            )}
    ].

changeset_errors_quoting_a_redacted_number_test() ->
    CS0 = kura_changeset:cast(?S, #{}, #{token => ~"t"}, [token]),
    CS = CS0#kura_changeset{
        changes = #{token => 4821973},
        errors = [{base, ~"value \"4821973\" is out of range"}, {base, ~"is 4 long"}]
    },
    ?assertEqual(
        [{base, ?R}, {base, ~"is 4 long"}], (kura_redact:changeset(CS))#kura_changeset.errors
    ).

changeset_params_under_binary_keys_test() ->
    CS0 = kura_changeset:cast(?S, #{}, #{email => ~"a@b.c"}, [email]),
    CS = CS0#kura_changeset{
        params = #{~"token" => ~"new", ~"email" => ~"a@b.c", 7 => ~"kept"}
    },
    ?assertEqual(
        #{~"token" => ?R, ~"email" => ~"a@b.c", 7 => ?R},
        (kura_redact:changeset(CS))#kura_changeset.params
    ).

%% A confirmation and a mistyped name hold what the field they stand for
%% holds, so in a schema with redacted fields only a field's key shows its
%% value.
changeset_params_under_keys_that_name_no_field_test() ->
    Params = #{
        ~"email" => ~"a@b.c",
        ~"token" => ~"tok-SECRET",
        ~"token_confirmation" => ~"tok-SECRET",
        ~"tokne" => ~"tok-TYPO",
        password_hsah => ~"hash-TYPO",
        "Email" => ~"cased-TYPO",
        ~"" => ~"empty-TYPO"
    },
    Shown = kura_redact:changeset((secret_changeset())#kura_changeset{params = Params}),
    ?assertEqual(
        #{
            ~"email" => ~"a@b.c",
            ~"token" => ?R,
            ~"token_confirmation" => ?R,
            ~"tokne" => ?R,
            password_hsah => ?R,
            "Email" => ?R,
            ~"" => ?R
        },
        Shown#kura_changeset.params
    ),
    Printed = iolist_to_binary(io_lib:format("~p", [Shown])),
    ?assertEqual(nomatch, binary:match(Printed, [~"SECRET", ~"TYPO"])).

changeset_params_that_name_a_field_test_() ->
    Bound = kura_test_enc_bound_schema,
    Owner = kura_changeset:cast(?OWNER, #{}, #{name => ~"n"}, [name]),
    [
        {"a virtual field is a field",
            ?_assertEqual(
                #{~"ssn" => ?R, ~"ssn_confirmation" => ~"shown", ~"ssn_confirmed" => ?R},
                (kura_redact:changeset(
                    (kura_changeset:cast(Bound, #{}, #{}, []))#kura_changeset{
                        params = #{
                            ~"ssn" => ~"1",
                            ~"ssn_confirmation" => ~"shown",
                            ~"ssn_confirmed" => ~"1"
                        }
                    }
                ))#kura_changeset.params
            )},
        {"an embed is one, and one whose schema redacts nothing is shown",
            ?_assertEqual(
                #{name => ~"n", address => #{city => ~"c"}, creds => ?R, adress => ?R},
                (kura_redact:changeset(
                    (kura_changeset:cast(kura_test_redact_embed_parent, #{}, #{}, []))#kura_changeset{
                        params = #{
                            name => ~"n",
                            address => #{city => ~"c"},
                            creds => #{api_secret => ~"emb-SECRET"},
                            adress => #{city => ~"c"}
                        }
                    }
                ))#kura_changeset.params
            )},
        {"the params of an association are read by its schema",
            ?_assertEqual(
                #{
                    ~"nmae" => ?R,
                    ~"accounts" => [
                        #{~"email" => ~"a@b.c", ~"token" => ?R, ~"token_confirmation" => ?R}
                    ],
                    account => #{email => ~"a@b.c", tokne => ?R}
                },
                (kura_redact:changeset(Owner#kura_changeset{
                    params = #{
                        ~"nmae" => ~"n",
                        ~"accounts" => [
                            #{
                                ~"email" => ~"a@b.c",
                                ~"token" => ~"tok-SECRET",
                                ~"token_confirmation" => ~"tok-SECRET"
                            }
                        ],
                        account => #{email => ~"a@b.c", tokne => ~"tok-SECRET"}
                    }
                }))#kura_changeset.params
            )},
        {"a schema with associations and no redacted field shows no value under another key",
            ?_assertEqual(
                #{
                    ~"name" => ~"n",
                    ~"acounts" => ?R,
                    ~"token_confirmation" => ?R
                },
                (kura_redact:changeset(Owner#kura_changeset{
                    params = #{
                        ~"name" => ~"n",
                        ~"acounts" => [#{~"token" => ~"tok-SECRET"}],
                        ~"token_confirmation" => ~"tok-SECRET"
                    }
                }))#kura_changeset.params
            )},
        {"an association of a schema with redacted fields is a name too",
            ?_assertEqual(
                #{
                    ~"name" => ~"n",
                    ~"api_key" => ?R,
                    ~"members" => [#{~"email" => ~"a@b.c", ~"token" => ?R}],
                    members => [#{email => ~"a@b.c", tokne => ?R}],
                    ~"membres" => ?R
                },
                (kura_redact:changeset(
                    (kura_changeset:cast(kura_test_secret_team_schema, #{}, #{}, []))#kura_changeset{
                        params = #{
                            ~"name" => ~"n",
                            ~"api_key" => ~"key-SECRET",
                            ~"members" => [#{~"email" => ~"a@b.c", ~"token" => ~"tok-SECRET"}],
                            members => [#{email => ~"a@b.c", tokne => ~"tok-SECRET"}],
                            ~"membres" => [#{~"email" => ~"a@b.c"}]
                        }
                    }
                ))#kura_changeset.params
            )},
        {"data and changes keep the keys that name no field",
            ?_test(begin
                CS = (secret_changeset())#kura_changeset{
                    data = #{id => 1, token => ~"t", total => 3},
                    changes = #{token => ~"t", rank => 2}
                },
                Shown = kura_redact:changeset(CS),
                ?assertEqual(#{id => 1, token => ?R, total => 3}, Shown#kura_changeset.data),
                ?assertEqual(#{token => ?R, rank => 2}, Shown#kura_changeset.changes)
            end)}
    ].

changeset_with_a_redacted_value_that_is_no_text_test() ->
    CS0 = kura_changeset:cast(?S, #{}, #{email => ~"a@b.c"}, [email]),
    CS = CS0#kura_changeset{
        changes = #{token => 1.5, password_hash => undefined},
        errors = [{base, ~"is 1.5"}]
    },
    Shown = kura_redact:changeset(CS),
    ?assertEqual(#{token => ?R, password_hash => ?R}, Shown#kura_changeset.changes),
    ?assertEqual([{base, ~"is 1.5"}], Shown#kura_changeset.errors).

schemaless_changeset_is_left_alone_test() ->
    CS = kura_changeset:cast(#{token => string}, #{}, #{token => ~"t"}, [token]),
    ?assertEqual(CS, kura_redact:changeset(CS)).

changeset_error_messages_that_are_no_binary_test_() ->
    CS = secret_changeset(),
    Errors = fun(Entries) ->
        (kura_redact:changeset(CS#kura_changeset{errors = ill_typed(Entries)}))#kura_changeset.errors
    end,
    [
        {"a string is shown as its text",
            ?_assertEqual([{email, ~"is not allowed"}], Errors([{email, "is not allowed"}]))},
        {"a string that quotes a redacted value",
            ?_assertEqual([{email, ?R}], Errors([{email, "is not tok-SECRET"}]))},
        {"text in pieces is shown joined",
            ?_assertEqual(
                [{email, ~"is not allowed"}], Errors([{email, ["is not ", [~"allow", $e], "d"]}])
            )},
        {"text that does not end as a list",
            ?_assertEqual([{email, ?R}], Errors([{email, improper($a, ~"b")}]))},
        {"a quote in pieces",
            ?_assertEqual([{email, ?R}], Errors([{email, ["is not tok-", [~"SECRET"]]}]))},
        {"an atom is shown as its text",
            ?_assertEqual([{email, ~"required"}], Errors([{email, required}]))},
        {"a message that is no text",
            ?_assertEqual(
                [{email, ?R}, {email, ?R}, {email, ?R}],
                Errors([{email, {invalid, ~"x"}}, {email, [~"is", not_text]}, {email, 1.5}])
            )},
        {"text that is not Unicode",
            ?_assertEqual([{email, ?R}], Errors([{email, [~"is ", <<255>>]}]))}
    ].

changeset_error_entries_of_another_shape_test_() ->
    CS = secret_changeset(),
    Errors = fun(Entries) ->
        (kura_redact:changeset(CS#kura_changeset{errors = ill_typed(Entries)}))#kura_changeset.errors
    end,
    [
        {"a longer tuple keeps its field",
            ?_assertEqual([{email, ?R}], Errors([{email, ~"is short", [{min, 3}]}]))},
        {"a tuple of one", ?_assertEqual([{email, ?R}], Errors([{email}]))},
        {"a field that is no atom",
            ?_assertEqual([{base, ?R}, {base, ?R}], Errors([{~"email", ~"is short"}, {7, 8, 9}]))},
        {"an entry that is no tuple",
            ?_assertEqual([{base, ?R}, {base, ?R}, {base, ?R}], Errors([~"is short", {}, email]))},
        {"errors that are no list", [
            ?_assertEqual([{base, ?R}], Errors(~"tok-SECRET is short")),
            ?_assertEqual([{base, ?R}], Errors(#{email => ~"is short"}))
        ]},
        {"a list that does not end",
            ?_assertEqual(
                [{email, ~"is short"}, {base, ?R}],
                Errors(improper({email, ~"is short"}, ~"tok-SECRET"))
            )}
    ].

changeset_with_nothing_to_quote_still_reads_its_errors_test() ->
    CS0 = kura_changeset:cast(?S, #{}, #{email => ~"a@b.c"}, [email]),
    CS = CS0#kura_changeset{errors = ill_typed([{email, ~"is short"}, {email, ~"x", y}, z])},
    ?assertEqual(
        [{email, ~"is short"}, {email, ?R}, {base, ?R}],
        (kura_redact:changeset(CS))#kura_changeset.errors
    ).

changeset_errors_quoting_a_redacted_value_that_is_no_binary_test_() ->
    CS = secret_changeset(),
    Errors = fun(Token) ->
        Shown = kura_redact:changeset(CS#kura_changeset{
            changes = #{token => Token},
            params = #{},
            errors = [{base, ~"is tok-OTHER, not 42"}, {base, ~"is invalid"}]
        }),
        Shown#kura_changeset.errors
    end,
    Quoted = [{base, ?R}, {base, ~"is invalid"}],
    [
        {"a string", ?_assertEqual(Quoted, Errors("tok-OTHER"))},
        {"a string in pieces", ?_assertEqual(Quoted, Errors(["tok-", ~"OTHER"]))},
        {"inside a list", ?_assertEqual(Quoted, Errors([~"tok-OTHER", 7.5]))},
        {"inside a tuple", ?_assertEqual(Quoted, Errors({wrapped, ~"tok-OTHER"}))},
        {"inside a map", [
            ?_assertEqual(Quoted, Errors(#{value => ~"tok-OTHER"})),
            ?_assertEqual(Quoted, Errors(#{~"tok-OTHER" => value}))
        ]},
        {"a number inside a list", ?_assertEqual(Quoted, Errors([42, 7.5]))},
        {"a list of one number, which reads as a string too", ?_assertEqual(Quoted, Errors([42]))},
        {"a list of numbers that all read as characters",
            ?_assertEqual(Quoted, Errors([100, 42, 101]))},
        {"after a list that does not end",
            ?_assertEqual(Quoted, Errors(improper(7.5, ~"tok-OTHER")))},
        {"nothing that can be quoted",
            ?_assertEqual(
                [{base, ~"is tok-OTHER, not 42"}, {base, ~"is invalid"}], Errors([7.5, {}, ""])
            )}
    ].

changeset_fields_that_are_no_map_test() ->
    CS = secret_changeset(),
    Shown = kura_redact:changeset(CS#kura_changeset{
        data = ill_typed([{token, ~"tok-SECRET"}]),
        errors = [{email, ~"is short"}]
    }),
    ?assertEqual(#{base => ?R}, Shown#kura_changeset.data),
    ?assertEqual(#{token => ?R}, Shown#kura_changeset.changes),
    ?assertEqual([{email, ?R}], Shown#kura_changeset.errors),
    ?assertEqual(
        #{base => ?R},
        (kura_redact:changeset(CS#kura_changeset{changes = ill_typed(~"tok-SECRET")}))#kura_changeset.changes
    ),
    ?assertEqual(
        #{base => ?R},
        (kura_redact:changeset(CS#kura_changeset{params = ill_typed({token, ~"tok-SECRET"})}))#kura_changeset.params
    ).

changeset_keys_that_are_no_atom_or_binary_test() ->
    CS = secret_changeset(),
    Shown = kura_redact:changeset(CS#kura_changeset{
        params = #{
            "token" => ~"tok-SECRET",
            "email" => ~"a@b.c",
            [~"tok", "en"] => ~"tok-SECRET",
            {token} => ~"tok-SECRET",
            [token] => ~"tok-SECRET"
        }
    }),
    ?assertEqual(
        #{
            "token" => ?R,
            "email" => ~"a@b.c",
            [~"tok", "en"] => ?R,
            {token} => ?R,
            [token] => ?R
        },
        Shown#kura_changeset.params
    ).

changeset_of_a_schema_that_cannot_be_read_test_() ->
    Hidden = #kura_changeset{valid = false, errors = [{base, ?R}]},
    CS = secret_changeset(),
    [
        {"a schema that is no atom",
            ?_assertEqual(
                Hidden, kura_redact:changeset(CS#kura_changeset{schema = ill_typed(~"accounts")})
            )},
        {"redact/0 that is no list of fields",
            ?_assertEqual(
                Hidden,
                kura_redact:changeset(CS#kura_changeset{schema = kura_test_bad_redact_schema})
            )},
        {"a module that does not exist",
            ?_assertEqual(Hidden, kura_redact:changeset(CS#kura_changeset{schema = not_a_schema}))},
        {"a module that is no schema",
            ?_assertEqual(Hidden, kura_redact:changeset(CS#kura_changeset{schema = lists}))},
        {"no changeset at all", [
            ?_assertEqual(Hidden, kura_redact:changeset(ill_typed(~"tok-SECRET"))),
            ?_assertEqual(
                Hidden, kura_redact:changeset(ill_typed({kura_changeset, true, ?S, #{}}))
            )
        ]}
    ].

nested_changesets_test() ->
    Child = secret_changeset(),
    Parent0 = kura_changeset:cast(?OWNER, #{}, #{name => ~"n"}, [name]),
    Parent = Parent0#kura_changeset{
        assoc_changes = ill_typed(#{
            account => Child,
            accounts => [Child, ~"tok-SECRET"],
            other => {token, ~"tok-SECRET"},
            more => improper(Child, ~"tok-SECRET")
        })
    },
    Hidden = #kura_changeset{valid = false, errors = [{base, ?R}]},
    ShownChild = kura_redact:changeset(Child),
    Shown = kura_redact:changeset(Parent),
    ?assertEqual(#{token => ?R}, ShownChild#kura_changeset.changes),
    ?assertEqual(
        #{
            account => ShownChild,
            accounts => [ShownChild, Hidden],
            other => Hidden,
            more => [ShownChild, Hidden]
        },
        Shown#kura_changeset.assoc_changes
    ),
    ?assertEqual(
        #{base => Hidden},
        (kura_redact:changeset(Parent#kura_changeset{assoc_changes = ill_typed([Child])}))#kura_changeset.assoc_changes
    ),
    Schemaless = kura_changeset:cast(#{name => string}, #{}, #{name => ~"n"}, [name]),
    ?assertEqual(
        #{account => ShownChild},
        (kura_redact:changeset(Schemaless#kura_changeset{assoc_changes = #{account => Child}}))#kura_changeset.assoc_changes
    ).

nested_values_of_an_association_test() ->
    Account = #{email => ~"a@b.c", token => ~"tok-SECRET"},
    ShownAccount = #{email => ~"a@b.c", token => ?R},
    Parent0 = kura_changeset:cast(?OWNER, #{}, #{name => ~"n"}, [name]),
    Parent = Parent0#kura_changeset{
        data = #{id => 1, accounts => [Account, not_loaded, 7], account => undefined},
        changes = #{name => ~"n", account => {token, ~"tok-SECRET"}},
        params = #{
            ~"name" => ~"n",
            ~"accounts" => improper(#{~"token" => ~"tok-SECRET"}, ~"tok-SECRET"),
            "account" => Account
        },
        errors = [{base, ~"tok-SECRET is taken"}, {name, ~"is short"}]
    },
    Shown = kura_redact:changeset(Parent),
    ?assertEqual(
        #{id => 1, accounts => [ShownAccount, not_loaded, ?R], account => undefined},
        Shown#kura_changeset.data
    ),
    ?assertEqual(#{name => ~"n", account => ?R}, Shown#kura_changeset.changes),
    ?assertEqual(
        #{~"name" => ~"n", ~"accounts" => [#{~"token" => ?R}, ?R], "account" => ShownAccount},
        Shown#kura_changeset.params
    ),
    ?assertEqual([{base, ?R}, {name, ~"is short"}], Shown#kura_changeset.errors),
    ?assertEqual(
        #{id => 1, accounts => [ShownAccount]},
        kura_redact:record(?OWNER, #{id => 1, accounts => [Account]})
    ).

%% A schema with nothing to hide gets back what it gave.
nothing_hidden_is_left_as_it_is_test() ->
    Plain = kura_test_plain_ssn_schema,
    CS0 = kura_changeset:cast(Plain, #{}, #{ssn => ~"1"}, [ssn]),
    CS = CS0#kura_changeset{
        data = ill_typed(#{7 => ~"kept"}),
        errors = ill_typed([{ssn, "is short"}, {ssn}])
    },
    ?assertEqual(CS, kura_redact:changeset(CS)),
    ?assertEqual(#{7 => ~"kept"}, kura_redact:record(Plain, ill_typed(#{7 => ~"kept"}))).

errors_that_quote_a_nested_value_test() ->
    Parent0 = kura_changeset:cast(?OWNER, #{}, #{name => ~"n"}, [name]),
    Parent = Parent0#kura_changeset{
        data = #{accounts => [#{token => ~"tok-MANY"}, not_loaded, []], account => undefined},
        changes = #{account => #{token => ~"tok-ONE"}},
        errors = [
            {base, ~"tok-MANY is taken"},
            {base, ~"tok-ONE is taken"},
            {name, ~"is undefined"},
            {name, ~"is not_loaded"}
        ]
    },
    ?assertEqual(
        [{base, ?R}, {base, ?R}, {name, ~"is undefined"}, {name, ~"is not_loaded"}],
        (kura_redact:changeset(Parent))#kura_changeset.errors
    ).

a_changeset_among_the_values_of_an_association_test() ->
    Child = secret_changeset(),
    Parent0 = kura_changeset:cast(?OWNER, #{}, #{name => ~"n"}, [name]),
    Parent = Parent0#kura_changeset{changes = ill_typed(#{accounts => [Child], account => Child})},
    ShownChild = kura_redact:changeset(Child),
    ?assertEqual(
        #{accounts => [ShownChild], account => ShownChild},
        (kura_redact:changeset(Parent))#kura_changeset.changes
    ).

update_all_params_of_a_redacted_schema_test_() ->
    Query = kura_query:where(kura_query:from(?S), {email, ~"a@b.c"}),
    Params = [~"new@b.c", ~"a@b.c"],
    [
        {"a set field that is not redacted",
            ?_assertEqual(
                {redacted, Params}, kura_redact:update_all_params(Query, [email], Params)
            )},
        {"a set field that is redacted",
            ?_assertEqual(
                {redacted, [?R, ?R]}, kura_redact:update_all_params(Query, [token], Params)
            )},
        {"a schema whose redacted fields cannot be read",
            ?_assertEqual(
                {redacted, [?R, ?R]},
                kura_redact:update_all_params(
                    kura_query:from(kura_test_bad_redact_schema), [name], Params
                )
            )}
    ].

record_that_cannot_be_read_test_() ->
    [
        {"no map",
            ?_assertEqual(
                #{base => ?R}, kura_redact:record(?S, ill_typed([{token, ~"tok-SECRET"}]))
            )},
        {"a schema that is no atom",
            ?_assertEqual(
                #{base => ?R}, kura_redact:record(ill_typed(~"accounts"), #{token => ~"t"})
            )},
        {"redact/0 that is no list of fields",
            ?_assertEqual(
                #{base => ?R}, kura_redact:record(kura_test_bad_redact_schema, #{token => ~"t"})
            )},
        {"no map, of a name that is no schema",
            ?_assertEqual(#{base => ?R}, kura_redact:record(not_a_schema, ill_typed(~"t")))},
        {"a module that does not exist",
            ?_assertEqual(#{base => ?R}, kura_redact:record(not_a_schema, #{token => ~"t"}))},
        {"a module that is no schema",
            ?_assertEqual(#{base => ?R}, kura_redact:record(lists, #{token => ~"t"}))},
        {"a record whose association names a module that is no schema",
            ?_assertEqual(
                #{id => 1, plain => #{base => ?R}},
                kura_redact:record(kura_test_stray_assoc_schema, #{id => 1, plain => #{id => 2}})
            )}
    ].

secret_changeset() ->
    kura_changeset:cast(
        ?S, #{id => 1}, #{token => ~"tok-SECRET", email => ~"a@b.c"}, [token]
    ).

-spec ill_typed(term()) -> dynamic().
ill_typed(Term) -> Term.

-spec improper(term(), term()) -> dynamic().
improper(Head, Tail) -> [Head | ill_typed(Tail)].

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
            {"a subquery over a redacted schema: every value", fun t_subquery_of_another_schema/0},
            {"a join with a redacted schema: every value", fun t_joined_schema/0},
            {"a CTE over a redacted schema: every value", fun t_cte/0},
            {"a subquery filtering on what an outer CTE hands it: every value", fun t_outer_cte/0},
            {"a subquery filtering on the outer query's redacted field: every value",
                fun t_correlated_subquery/0},
            {"a plain field named as a redacted one of another source: every value",
                fun t_same_name/0},
            {"a distance from a redacted vector: every value", fun t_distance/0},
            {"a failed statement's error keeps its code, not its text", fun t_error_reason/0},
            {"error detail is opt-in, never for a redacted schema", fun t_error_detail/0},
            {"a telemetry_error_detail that is not a boolean shows no detail",
                fun t_error_detail_not_a_boolean/0},
            {"a database message is not put on a changeset of a redacting schema",
                fun t_base_error/0},
            {"raw SQL is shown as it is unless marked sensitive", fun t_raw_sql/0},
            {"a stream's cursor statement: every value when it filters on one", fun t_stream/0}
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

t_subquery_of_another_schema() ->
    Plain = kura_query:from(kura_test_edge_schema),
    ByToken = kura_query:select(
        kura_query:where(kura_query:from(?S), {token, '=', ~"t"}), [id]
    ),
    In = kura_query:where(Plain, {id, in, {subquery, ByToken}}),
    ?assertEqual({[[?R]], [[~"t"]]}, shown(fun() -> kura_repo_worker:all(?REPO, In) end)),
    Exists = kura_query:where(
        Plain, {'or', [{name, '=', ~"n"}, {'not', {exists, {subquery, ByToken}}}]}
    ),
    ?assertEqual(
        {[[?R, ?R]], [[~"n", ~"t"]]}, shown(fun() -> kura_repo_worker:all(?REPO, Exists) end)
    ),
    ByEmail = kura_query:select(
        kura_query:where(kura_query:from(?S), {email, '=', ~"a@b.c"}), [id]
    ),
    Others = kura_query:where(Plain, {id, in, {subquery, ByEmail}}),
    ?assertEqual(
        {[[~"a@b.c"]], [[~"a@b.c"]]}, shown(fun() -> kura_repo_worker:all(?REPO, Others) end)
    ),
    ?assertEqual(
        {[[?R]], [[~"t"]]},
        shown(fun() -> kura_repo_worker:delete_all(?REPO, In) end)
    ).

t_joined_schema() ->
    Joined = kura_query:join(kura_query:from(kura_test_edge_schema), inner, ?S, {id, id}),
    ByToken = kura_query:where(Joined, {token, '=', ~"t"}),
    ?assertEqual({[[?R]], [[~"t"]]}, shown(fun() -> kura_repo_worker:all(?REPO, ByToken) end)),
    ByName = kura_query:where(Joined, {name, '=', ~"n"}),
    ?assertEqual({[[~"n"]], [[~"n"]]}, shown(fun() -> kura_repo_worker:all(?REPO, ByName) end)).

t_cte() ->
    Body = kura_query:from(?S),
    Q = kura_query:where(
        kura_query:with_cte(kura_query:from(~"recent"), ~"recent", Body), {token, '=', ~"t"}
    ),
    ?assertEqual({[[?R]], [[~"t"]]}, shown(fun() -> kura_repo_worker:all(?REPO, Q) end)).

twice_hidden(Query, Given) ->
    Hidden = {[[?R || _ <- Given]], [Given]},
    ?assertEqual(hidden, kura_redact:mode(Query)),
    ?assertEqual(Hidden, shown(fun() -> kura_repo_worker:all(?REPO, Query) end)),
    ?assertEqual(Hidden, shown(fun() -> kura_repo_worker:all(?REPO, Query) end)).

t_outer_cte() ->
    ByToken = kura_query:select(
        kura_query:where(kura_query:from(~"a"), {token, '=', ~"tok"}), [id]
    ),
    Q = kura_query:with_cte(
        kura_query:where(kura_query:from(kura_test_shared_schema), {id, in, {subquery, ByToken}}),
        ~"a",
        kura_query:select(kura_query:from(?S), [id, token])
    ),
    twice_hidden(Q, [~"tok"]).

t_correlated_subquery() ->
    Sub = kura_query:where(kura_query:from(kura_test_shared_schema), {token, '=', ~"tok"}),
    twice_hidden(kura_query:where(kura_query:from(?S), {exists, {subquery, Sub}}), [~"tok"]).

t_same_name() ->
    Named = kura_query:where(kura_query:from(~"audit_rows"), {token, '=', ~"plain"}),
    Q = kura_query:where(kura_query:from(?S), {id, in, {subquery, kura_query:select(Named, [id])}}),
    twice_hidden(Q, [~"plain"]),
    Alone = kura_query:where(kura_query:from(~"audit_rows"), {token, '=', ~"plain"}),
    ?assertEqual(
        {[[~"plain"]], [[~"plain"]]}, shown(fun() -> kura_repo_worker:all(?REPO, Alone) end)
    ).

t_distance() ->
    From = kura_query:from(kura_test_secret_vector_schema),
    D = kura_query:distance(embedding, l2, [1, 0, 0]),
    Vector = ~"[1.0,0.0,0.0]",
    twice_hidden(kura_query:where(From, {D, '<', 0.5}), [Vector, 0.5]),
    twice_hidden(kura_query:order_by(From, [{D, asc}]), [Vector]),
    twice_hidden(kura_query:select_expr(From, [{id, id}, {dist, D}]), [Vector]),
    ByContent = kura_query:where(From, {content, '=', ~"c"}),
    ?assertEqual(
        {[[~"c"]], [[~"c"]]}, shown(fun() -> kura_repo_worker:all(?REPO, ByContent) end)
    ).

t_base_error() ->
    Quoting = #{
        code => ~"22P02", message => ~"invalid input syntax for type uuid: \"tok-SECRET\""
    },
    kura_test_recording_driver:reply_with([
        {error, Quoting}, {error, Quoting}, {error, {closed, ~"tok-SECRET"}}, {error, Quoting}
    ]),
    {error, #kura_changeset{errors = Inserted}} = kura_repo_worker:insert(
        ?REPO, changeset(#{}, #{email => ~"a@b.c", token => ~"tok-SECRET"})
    ),
    ?assertEqual([{base, ~"database error 22P02"}], Inserted),
    {error, #kura_changeset{errors = Updated}} = kura_repo_worker:update(
        ?REPO, changeset(#{id => 1}, #{token => ~"tok-SECRET"})
    ),
    ?assertEqual([{base, ~"database error 22P02"}], Updated),
    {error, #kura_changeset{errors = Closed}} = kura_repo_worker:insert(
        ?REPO, changeset(#{}, #{email => ~"a@b.c", token => ~"tok-SECRET"})
    ),
    ?assertEqual([{base, ~"database error"}], Closed),
    {error, #kura_changeset{errors = Plain}} = insert_plain(),
    ?assertEqual([{base, maps:get(message, Quoting)}], Plain).

duplicate_error() ->
    #{
        code => ~"23505",
        constraint => ~"accounts_token_key",
        message => ~"duplicate key value violates unique constraint",
        detail => ~"Key (token)=(t) already exists."
    }.

t_error_reason() ->
    Error = duplicate_error(),
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
    {_, [Plain], _} = observed(fun insert_plain/0),
    ?assertEqual(
        #{code => ~"23505", constraint => ~"accounts_token_key"}, maps:get(error_reason, Plain)
    ).

t_error_detail() ->
    Error = duplicate_error(),
    application:set_env(kura, telemetry_error_detail, true),
    try
        kura_test_recording_driver:reply_with([{error, Error}, {error, Error}]),
        {_, [Plain], _} = observed(fun insert_plain/0),
        ?assertEqual(Error, maps:get(error_reason, Plain)),
        {_, [Secret], _} = observed(fun() ->
            kura_repo_worker:insert(?REPO, changeset(#{}, #{email => ~"a@b.c"}))
        end),
        ?assertEqual(
            #{code => ~"23505", constraint => ~"accounts_token_key"},
            maps:get(error_reason, Secret)
        )
    after
        application:unset_env(kura, telemetry_error_detail)
    end.

t_error_detail_not_a_boolean() ->
    Error = duplicate_error(),
    application:set_env(kura, telemetry_error_detail, yes),
    try
        kura_test_recording_driver:reply_with([{error, Error}]),
        {_, [Plain], _} = observed(fun insert_plain/0),
        ?assertEqual(
            #{code => ~"23505", constraint => ~"accounts_token_key"},
            maps:get(error_reason, Plain)
        )
    after
        application:unset_env(kura, telemetry_error_detail)
    end.

insert_plain() ->
    kura_repo_worker:insert(
        ?REPO, kura_changeset:cast(kura_test_edge_schema, #{}, #{name => ~"n"}, [name])
    ).

t_raw_sql() ->
    SQL = ~"SELECT 1 FROM accounts WHERE token = $1",
    ?assertEqual(
        {[[~"t"]], [[~"t"]]}, shown(fun() -> kura_repo_worker:query(?REPO, SQL, [~"t"]) end)
    ),
    ?assertEqual(
        {[[~"t"]], [[~"t"]]},
        shown(fun() -> kura_repo_worker:query(?REPO, SQL, [~"t"], #{sensitive => false}) end)
    ),
    ?assertEqual(
        {[[?R]], [[~"t"]]},
        shown(fun() -> kura_repo_worker:query(?REPO, SQL, [~"t"], #{sensitive => true}) end)
    ),
    Error = {pgsql_error, #{code => ~"22P02", message => ~"invalid input syntax: \"t\""}},
    application:set_env(kura, telemetry_error_detail, true),
    try
        kura_test_recording_driver:reply_with([{error, Error}]),
        {Result, [Event], _} = observed(fun() ->
            kura_repo_worker:query(?REPO, SQL, [~"t"], #{sensitive => true})
        end),
        ?assertEqual({error, Error}, Result),
        ?assertEqual({pgsql_error, #{code => ~"22P02"}}, maps:get(error_reason, Event))
    after
        application:unset_env(kura, telemetry_error_detail)
    end.

t_stream() ->
    Each = fun(_Rows) -> ok end,
    ByToken = kura_query:where(kura_query:from(?S), {token, '=', ~"t"}),
    {ok, [Declared | _], [[~"t"] | _]} = observed(fun() ->
        kura_stream:stream(?REPO, ByToken, Each, #{})
    end),
    ?assertEqual([?R], maps:get(params, Declared)),
    ?assertEqual(~"accounts", maps:get(source, Declared)),
    ByEmail = kura_query:where(kura_query:from(?S), {email, '=', ~"a@b.c"}),
    {ok, [Shown | _], _} = observed(fun() -> kura_stream:stream(?REPO, ByEmail, Each, #{}) end),
    ?assertEqual([~"a@b.c"], maps:get(params, Shown)).

write_params_test_() ->
    [
        {"a table, or a schema with nothing redacted: as they are", [
            ?_assertEqual([~"t"], kura_redact:write_params(~"accounts", [token], [~"t"])),
            ?_assertEqual(
                [~"n"], kura_redact:write_params(kura_test_edge_schema, [name], [~"n"])
            )
        ]},
        {"values by field, row after row",
            ?_assertEqual(
                {redacted, [~"a", ?R, ~"b", ?R]},
                kura_redact:write_params(?S, [email, token], [~"a", ~"t", ~"b", ~"u"])
            )},
        {"values that do not divide into rows: every value",
            ?_assertEqual(
                {redacted, [?R, ?R, ?R]},
                kura_redact:write_params(?S, [email, token], [~"a", ~"t", ~"b"])
            )},
        {"no field named: every value",
            ?_assertEqual({redacted, [?R]}, kura_redact:write_params(?S, [], [~"t"]))},
        {"values somewhere among the parameters: every value when one is redacted", [
            ?_assertEqual(
                {redacted, [?R, ?R]},
                kura_redact:write_params(?S, {touches, [id, token]}, [1, ~"t"])
            ),
            ?_assertEqual(
                {redacted, [1, ~"a"]},
                kura_redact:write_params(?S, {touches, [id, email]}, [1, ~"a"])
            )
        ]}
    ].

update_all_params_test_() ->
    Plain = kura_query:from(kura_test_edge_schema),
    ByToken = kura_query:select(kura_query:where(kura_query:from(?S), {token, '=', ~"t"}), [id]),
    [
        ?_assertEqual(
            [~"n", 1],
            kura_redact:update_all_params(
                kura_query:where(Plain, {id, '=', 1}), [name], [~"n", 1]
            )
        ),
        ?_assertEqual(
            {redacted, [?R, ?R]},
            kura_redact:update_all_params(
                kura_query:where(Plain, {id, in, {subquery, ByToken}}), [name], [~"n", ~"t"]
            )
        )
    ].

mode_test_() ->
    From = kura_query:from(kura_test_secret_vector_schema),
    Secret = {distance, embedding, l2, ~"[1.0,0.0,0.0]"},
    Other = {distance, content, l2, ~"[1.0,0.0,0.0]"},
    Row = kura_query:from(?S),
    [
        ?_assertEqual(plain, kura_redact:mode(kura_query:from(kura_test_edge_schema))),
        ?_assertEqual(redacted, kura_redact:mode(From)),
        {"a distance after one from another field", [
            ?_assertEqual(
                hidden,
                kura_redact:mode(From#kura_query{order_bys = [{Other, asc}, {Secret, asc}]})
            ),
            ?_assertEqual(
                redacted,
                kura_redact:mode(From#kura_query{order_bys = [{Other, asc}, {id, desc}]})
            ),
            ?_assertEqual(
                hidden,
                kura_redact:mode(From#kura_query{select = {exprs, [{near, Other}, {far, Secret}]}})
            ),
            ?_assertEqual(
                redacted,
                kura_redact:mode(From#kura_query{select = {exprs, [{near, Other}, {id, id}]}})
            )
        ]},
        {"a condition on several fields at once", [
            ?_assertEqual(
                hidden,
                kura_redact:mode(Row#kura_query{wheres = [{[email, token], in, [[~"a", ~"t"]]}]})
            ),
            ?_assertEqual(
                redacted,
                kura_redact:mode(Row#kura_query{wheres = [{[email, id], in, [[~"a", 1]]}]})
            )
        ]}
    ].

sensitive_params_test() ->
    ?assertEqual({redacted, [?R, ?R]}, kura_redact:sensitive_params([~"t", 1])),
    ?assertEqual({redacted, []}, kura_redact:sensitive_params([])).

parameters_that_are_no_list_test_() ->
    Query = kura_query:where(kura_query:from(?S), {token, '=', ~"t"}),
    [
        ?_assertEqual({redacted, [?R]}, kura_redact:sensitive_params(ill_typed(~"t"))),
        ?_assertEqual(
            {redacted, [?R, ?R]}, kura_redact:sensitive_params(improper(~"t", ~"u"))
        ),
        ?_assertEqual({redacted, [?R]}, kura_redact:query_params(Query, ill_typed(~"t"))),
        ?_assertEqual(
            {redacted, [?R, ?R]},
            kura_redact:write_params(?S, [email, token], improper(~"a", ~"t"))
        ),
        ?_assertEqual(
            {redacted, [?R, ?R]},
            kura_redact:write_params(?S, {touches, [token]}, improper(~"a", ~"t"))
        ),
        ?_assertEqual(
            {redacted, [?R]},
            kura_redact:write_params(?S, {touches, [email]}, ill_typed(~"t"))
        ),
        ?_assertEqual(
            {redacted, [?R]},
            kura_redact:update_all_params(Query, [email], ill_typed(~"t"))
        )
    ].

fields_that_are_no_list_of_fields_test_() ->
    ByEmail = kura_query:where(kura_query:from(?S), {email, '=', ~"a"}),
    [
        ?_assertEqual(
            {redacted, [?R, ?R]}, kura_redact:write_params(?S, ill_typed(token), [~"a", ~"t"])
        ),
        ?_assertEqual(
            {redacted, [?R, ?R]},
            kura_redact:write_params(?S, improper(email, token), [~"a", ~"t"])
        ),
        ?_assertEqual(
            {redacted, [?R, ?R]},
            kura_redact:write_params(?S, {touches, ill_typed(token)}, [~"a", ~"t"])
        ),
        ?_assertEqual(
            {redacted, [?R]},
            kura_redact:write_params(kura_test_bad_redact_schema, [token], [~"t"])
        ),
        ?_assertEqual(
            {redacted, [?R]}, kura_redact:write_params(ill_typed({accounts}), [token], [~"t"])
        ),
        ?_assertEqual(
            {redacted, [?R, ?R]},
            kura_redact:update_all_params(ByEmail, ill_typed(token), [~"t", ~"a"])
        )
    ].

a_mode_or_query_that_cannot_be_read_test_() ->
    [
        ?_assertEqual({redacted, [?R]}, kura_redact:shown(ill_typed(shown), [~"t"])),
        ?_assertEqual({redacted, [?R]}, kura_redact:shown(redacted, ill_typed(~"t"))),
        ?_assertEqual({redacted, [?R]}, kura_redact:shown(plain, ill_typed(~"t"))),
        ?_assertEqual(hidden, kura_redact:mode(ill_typed({kura_query, ~"t"}))),
        ?_assertEqual(
            hidden, kura_redact:mode(kura_query:from(?S), ill_typed(~"t"))
        ),
        {"a source whose redact/0 is no list of fields", [
            ?_assertEqual(hidden, kura_redact:mode(kura_query:from(kura_test_bad_redact_schema))),
            ?_assertEqual(
                hidden,
                kura_redact:mode(
                    kura_query:join(
                        kura_query:from(kura_test_edge_schema),
                        inner,
                        kura_test_bad_redact_schema,
                        {id, id}
                    )
                )
            )
        ]},
        ?_assertEqual(
            {redacted, [?R]}, kura_redact:query_params(ill_typed({kura_query, ~"t"}), [~"t"])
        ),
        ?_assertEqual(
            {redacted, [?R]},
            kura_redact:update_all_params(ill_typed({kura_query, ~"t"}), [email], [~"t"])
        )
    ].

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
%% Embeds
%%----------------------------------------------------------------------

-define(EMBEDS, kura_test_redact_embed_parent).

embedded() ->
    #{
        id => 1,
        name => ~"n",
        creds => #{api_key => ~"key", api_secret => ~"emb-SECRET"},
        thread => [#{note => ~"a", replies => [], creds => #{api_secret => ~"emb-SECRET"}}],
        address => #{city => ~"c"}
    }.

an_embed_whose_schema_redacts_a_field_is_redacted_whole_test_() ->
    Record = embedded(),
    Hidden = Record#{creds := ?R, thread := ?R},
    Changeset = kura_changeset:cast_embed(
        kura_changeset:cast(?EMBEDS, #{}, Record, [name]), creds
    ),
    [
        {"the embeds are among the schema's redacted fields", [
            ?_assertEqual([creds, thread], kura_schema:redacted_fields(?EMBEDS)),
            ?_assertEqual(
                [creds, replies], kura_schema:redacted_fields(kura_test_redact_embed_middle)
            ),
            ?_assertEqual([], kura_schema:redacted_fields(kura_test_profile))
        ]},
        {"a record", ?_assertEqual(Hidden, kura_redact:record(?EMBEDS, Record))},
        {"a changeset", fun() ->
            Shown = kura_redact:changeset(Changeset#kura_changeset{
                errors = [{base, ~"cannot store emb-SECRET"}, {name, ~"is short"}]
            }),
            ?assertEqual(?R, maps:get(creds, Shown#kura_changeset.changes)),
            ?assertEqual(?R, maps:get(creds, Shown#kura_changeset.params)),
            ?assertEqual([{base, ?R}, {name, ~"is short"}], Shown#kura_changeset.errors)
        end},
        {"the audit log",
            ?_assertEqual(
                {undefined, Hidden, undefined},
                kura_audit:build_audit_data(?EMBEDS, insert, Record)
            )},
        {"the parameters of a write",
            ?_assertEqual(
                {redacted, [?R, ~"n", ?R]},
                kura_redact:write_params(?EMBEDS, [creds, name, thread], [~"{}", ~"n", ~"[]"])
            )},
        {"a query that filters on the embed",
            ?_assertEqual(
                hidden,
                kura_redact:mode(kura_query:where(kura_query:from(?EMBEDS), {creds, ~"{}"}))
            )},
        {"a query that filters on another field",
            ?_assertEqual(
                redacted, kura_redact:mode(kura_query:where(kura_query:from(?EMBEDS), {id, 1}))
            )}
    ].

an_insert_with_a_redacting_embed_test_() ->
    {setup,
        fun() ->
            kura_test_recording_driver:configure(?REPO),
            kura_query_cache:init()
        end,
        fun(_) -> kura_test_recording_driver:unconfigure() end, fun() ->
            Changeset = kura_changeset:cast_embed(
                kura_changeset:cast(?EMBEDS, #{}, maps:with([name, creds], embedded()), [name]),
                creds
            ),
            {[Shown], [Given]} = shown(fun() -> kura_repo_worker:insert(?REPO, Changeset) end),
            ?assertEqual(lists:sort([~"n", ?R]), lists:sort(Shown)),
            ?assertEqual(1, length([Param || Param <- Given, is_secret(Param)]))
        end}.

is_secret(Param) when is_binary(Param) -> binary:match(Param, ~"emb-SECRET") =/= nomatch;
is_secret(_Param) -> false.

%%----------------------------------------------------------------------
%% Audit log
%%----------------------------------------------------------------------

audit_test() ->
    Record = #{id => 1, email => ~"a@b.c", password_hash => ~"h", token => ~"t"},
    ?assertEqual(
        {undefined, #{id => 1, email => ~"a@b.c", password_hash => ?R, token => ?R}, undefined},
        kura_audit:build_audit_data(?S, insert, Record)
    ).
