-module(kura_redact_prop_tests).
-moduledoc """
Properties of the helpers that make a changeset, a record or a statement's
parameters safe to log: they return whatever they are given, and a value
they are to hide is in nothing they return.
""".

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(NUMTESTS, 300).
-define(S, kura_test_secret_schema).
-define(OWNER, kura_test_secret_owner_schema).
-define(MARKER, ~"s3cr3t-MARKER-q7").

property_test_() ->
    {timeout, 300, [
        {"a changeset of any terms is returned as a changeset", fun() ->
            run(prop_any_changeset())
        end},
        {"a redacted value is nowhere in the changeset returned", fun() ->
            run(prop_changeset_hides())
        end},
        {"a record of any terms is returned as a map without its redacted values", fun() ->
            run(prop_record_hides())
        end},
        {"parameters of any terms are shown, and hidden ones are not in what is shown", fun() ->
            run(prop_params())
        end},
        {"any error has a reason that can be shown", fun() -> run(prop_any_error_reason()) end}
    ]}.

marker_model_test_() ->
    [
        ?_assert(holds_marker(?MARKER)),
        ?_assert(holds_marker(<<$x, ?MARKER/binary, 0>>)),
        ?_assert(holds_marker(binary_to_list(?MARKER))),
        ?_assert(holds_marker([~"s3cr3t-", "MARKER", [$-, ~"q7"]])),
        ?_assert(holds_marker({a, [1.5, #{k => realised({improper, [x], ?MARKER})}]})),
        ?_assert(holds_marker(#{?MARKER => 1})),
        ?_assertNot(holds_marker({a, [1.5, #{k => [x, ~"s3cr3t"]}], ~"MARKER-q7"}))
    ].

run(Prop) ->
    ?assert(proper:quickcheck(Prop, [?NUMTESTS, {to_file, user}])).

%%----------------------------------------------------------------------
%% Properties
%%----------------------------------------------------------------------

prop_any_changeset() ->
    ?FORALL(
        Changeset,
        oneof([
            gen_term(), gen_tuple_of(gen_any_field()), gen_changeset(gen_term(), gen_any_field())
        ]),
        is_record(shown_changeset(realised(Changeset)), kura_changeset)
    ).

prop_changeset_hides() ->
    ?FORALL(
        Changeset,
        ?LET(Schema, gen_hiding_schema(), gen_hiding_changeset(Schema, gen_assocs())),
        begin
            Shown = shown_changeset(realised(Changeset)),
            is_record(Shown, kura_changeset) andalso not holds_marker(Shown)
        end
    ).

prop_record_hides() ->
    ?FORALL(
        {Schema, Record},
        ?LET(
            Schema,
            oneof([gen_hiding_schema(), gen_term()]),
            {Schema, oneof([gen_field(Schema), gen_no_map(), gen_term()])}
        ),
        begin
            Shown = shown_record(Schema, realised(Record)),
            is_map(Shown) andalso (not hides(Schema) orelse not holds_marker(Shown))
        end
    ).

prop_params() ->
    ?FORALL(
        {ParamsPlan, Mode, FieldsPlan, Query},
        {gen_params(), gen_mode(), gen_fields(), gen_query()},
        begin
            Params = realised(ParamsPlan),
            Fields = realised(FieldsPlan),
            Hidden = [
                shown(hidden, Params),
                shown(ill_typed({Mode}), Params),
                sensitive_params(Params),
                query_params(ill_typed({Query}), Params),
                write_params(ill_typed({?S}), Fields, Params),
                write_params(kura_test_bad_redact_schema, Fields, Params),
                write_params(?S, [], Params),
                write_params(?S, {touches, [token | Fields]}, Params),
                update_all_params(kura_query:from(?S), [token | Fields], Params),
                update_all_params(ill_typed({Query}), Fields, Params)
            ],
            Any = [
                shown(Mode, Params),
                query_params(Query, Params),
                write_params(?S, Fields, Params),
                write_params(?S, {touches, Fields}, Params),
                write_params(~"accounts", Fields, Params),
                update_all_params(Query, Fields, Params),
                mode(Query, Fields)
            ],
            lists:all(fun is_shown/1, Hidden) andalso
                not holds_marker(Hidden) andalso
                is_list(Any)
        end
    ).

prop_any_error_reason() ->
    ?FORALL(
        Reason,
        oneof([gen_term(), gen_error()]),
        not holds_marker(kura_redact:error_reason(Reason))
    ).

is_shown({redacted, Shown}) -> proper_list(Shown);
is_shown(_NotRedacted) -> false.

proper_list([_ | Rest]) -> proper_list(Rest);
proper_list(Rest) -> Rest =:= [].

hides(Schema) -> lists:member(Schema, hiding_schemas()).

hiding_schemas() -> [?S, ?OWNER, ~"accounts", {?S}, kura_test_bad_redact_schema].

%%----------------------------------------------------------------------
%% Model: does a term hold the marker, as text or in pieces
%%----------------------------------------------------------------------

holds_marker(Term) when is_binary(Term) ->
    binary:match(Term, ?MARKER) =/= nomatch;
holds_marker(Term) when is_list(Term) ->
    holds_marker(flat_text(Term)) orelse in_elements(Term);
holds_marker(Term) when is_tuple(Term) ->
    in_elements(tuple_to_list(Term));
holds_marker(Term) when is_map(Term) ->
    in_elements(maps:keys(Term)) orelse in_elements(maps:values(Term));
holds_marker(_Term) ->
    false.

in_elements([Term | Rest]) -> holds_marker(Term) orelse in_elements(Rest);
in_elements(Tail) -> Tail =/= [] andalso holds_marker(Tail).

flat_text(Chars) ->
    try unicode:characters_to_binary(Chars) of
        Text when is_binary(Text) -> Text;
        _Incomplete -> <<>>
    catch
        error:badarg -> <<>>
    end.

%%----------------------------------------------------------------------
%% Generators
%%
%% PropEr reads a generated list as a list of generators, and that cannot
%% be an improper one. A generator says {improper, Elements, Tail} for a
%% list that does not end, and realised/1 makes it one.
%%----------------------------------------------------------------------

gen_term() ->
    frequency([
        {1, resize(4, term())},
        {6,
            oneof([
                integer(),
                float(),
                binary(),
                boolean(),
                undefined,
                [],
                {},
                ~"",
                "",
                "text",
                [~"io", "list"],
                {integer(), binary()},
                oneof([base, token, email, kura_changeset, not_a_schema])
            ])}
    ]).

gen_few(Generator) ->
    ?LET(Count, range(0, 3), vector(Count, Generator)).

realised({improper, Elements, Tail}) ->
    realised(Elements) ++ realised(Tail);
realised(Term) when is_tuple(Term) ->
    list_to_tuple(realised(tuple_to_list(Term)));
realised(Term) when is_list(Term) ->
    [realised(Element) || Element <- Term];
realised(Term) when is_map(Term) ->
    #{realised(Key) => realised(Value) || Key := Value <- Term};
realised(Term) ->
    Term.

gen_hiding_schema() -> oneof(hiding_schemas()).

gen_changeset(SchemaGen, FieldGen) ->
    ?LET(
        {Schema, Valid, Data, Params, Changes, Errors, Assocs, Rest},
        {SchemaGen, gen_term(), FieldGen, FieldGen, FieldGen, gen_errors(), FieldGen, gen_rest()},
        changeset_tuple(Schema, Valid, Data, Params, Changes, Errors, Assocs, Rest)
    ).

%% An error can quote the marker, which is hidden only where it is a
%% redacted value. So the changes hold it as one whenever they are a map.
gen_hiding_changeset(Schema, AssocsGen) ->
    ?LET(
        {Valid, Data, Params, Changes, Errors, Assocs, Rest},
        {
            gen_term(),
            gen_field(Schema),
            gen_field(Schema),
            oneof([gen_hiding_map(Schema), gen_no_map()]),
            gen_errors(),
            AssocsGen,
            gen_rest()
        },
        changeset_tuple(Schema, Valid, Data, Params, Changes, Errors, Assocs, Rest)
    ).

%% The record's fields after errors: types, required, action, constraints,
%% assoc_changes, prepare, optimistic_lock.
changeset_tuple(
    Schema, Valid, Data, Params, Changes, Errors, Assocs, [
        Types, Required, Action, Constraints, Prepare, Lock
    ]
) ->
    {kura_changeset, Valid, Schema, Data, Params, Changes, Errors, Types, Required, Action,
        Constraints, Assocs, Prepare, Lock}.

gen_rest() -> vector(6, gen_term()).

gen_tuple_of(FieldGen) ->
    ?LET(
        Count,
        range(0, 14),
        ?LET(Fields, vector(Count, FieldGen), list_to_tuple([kura_changeset | Fields]))
    ).

gen_any_field() ->
    oneof([gen_term(), gen_field(?S), gen_field(?OWNER), gen_errors(), gen_assocs()]).

gen_field(Schema) ->
    frequency([{5, gen_field_map(Schema)}, {1, gen_no_map()}]).

gen_hiding_map(?OWNER) ->
    ?LET(Map, gen_field_map(?OWNER), Map#{accounts => [#{token => ?MARKER}]});
gen_hiding_map(Schema) ->
    ?LET(Map, gen_field_map(Schema), Map#{token => ?MARKER}).

%% The owner hides nothing of its own: the marker is in its associations'
%% records, and under keys that name no field. A schema that is not the
%% owner has no association, so what such a key holds is shown as it is.
gen_field_map(?OWNER) ->
    ?LET(
        {Shown, Hidden, Nested},
        {
            gen_few({gen_shown_key(), gen_term()}),
            gen_few({gen_unreadable_key(), gen_secret()}),
            gen_few({gen_assoc_key(), gen_nested()})
        },
        maps:from_list(Shown ++ Nested ++ Hidden)
    );
gen_field_map(_Schema) ->
    ?LET(
        {Shown, Hidden, Nested},
        {
            gen_few({gen_shown_key(), gen_term()}),
            gen_few({oneof([gen_redacted_key(), gen_unreadable_key()]), gen_secret()}),
            gen_few({
                gen_assoc_key(), oneof([undefined, gen_shown_record(), gen_few(gen_shown_record())])
            })
        },
        maps:from_list(Shown ++ Nested ++ Hidden)
    ).

gen_shown_key() -> oneof([id, email, name, ~"email", ~"name", "email"]).

gen_redacted_key() ->
    oneof([token, password_hash, ~"token", ~"password_hash", "token", [~"tok", "en"]]).

gen_unreadable_key() -> oneof([{token}, [token], 7, 1.5]).

gen_assoc_key() -> oneof([accounts, account, ~"accounts", "account"]).

gen_nested() ->
    oneof([
        undefined,
        not_loaded,
        gen_field_map(?S),
        gen_few(gen_field_map(?S)),
        {improper, gen_few(gen_field_map(?S)), ?MARKER},
        gen_no_map(),
        {wrapped, ?MARKER},
        {improper, [1.5], ?MARKER},
        [{a, [?MARKER, x]}]
    ]).

gen_shown_record() ->
    ?LET(Shown, gen_few({gen_shown_key(), gen_term()}), maps:from_list(Shown)).

gen_secret() ->
    oneof([
        ?MARKER,
        binary_to_list(?MARKER),
        [?MARKER],
        [~"s3cr3t-", "MARKER-q7"],
        {wrapped, ?MARKER},
        #{inner => ?MARKER},
        #{?MARKER => inner},
        {improper, [1.5], ?MARKER},
        [{a, [?MARKER, x]}]
    ]).

gen_no_map() ->
    oneof([
        ?MARKER,
        [{token, ?MARKER}],
        {token, ?MARKER},
        {improper, [token], ?MARKER},
        binary_to_list(?MARKER)
    ]).

gen_errors() ->
    oneof([
        gen_few(gen_error_entry()),
        {improper, gen_few(gen_error_entry()), ?MARKER},
        ?MARKER,
        {base, ?MARKER},
        #{base => ?MARKER}
    ]).

gen_error_entry() ->
    oneof([
        {gen_field_name(), ~"is invalid"},
        {gen_field_name(), "is invalid"},
        {gen_field_name(), required},
        {gen_field_name(), gen_term()},
        {gen_field_name(), gen_quote()},
        {gen_field_name(), gen_quote(), gen_term()},
        {gen_field_name(), ~"is invalid", gen_quote()},
        {gen_quote(), ~"is invalid"},
        {gen_quote()},
        gen_quote(),
        gen_term()
    ]).

gen_field_name() -> oneof([base, email, token, name]).

gen_quote() ->
    oneof([
        <<$", ?MARKER/binary, $">>,
        "is " ++ binary_to_list(?MARKER),
        [~"is s3cr3t-", ["MARKER", [~"-q7"]]],
        [~"is ", ?MARKER, not_text],
        {invalid, ?MARKER},
        ?MARKER
    ]).

gen_assocs() ->
    oneof([
        #{},
        ?LET(
            Entries,
            gen_few({oneof([accounts, account, other, ~"accounts", 7]), gen_assoc()}),
            maps:from_list(Entries)
        ),
        gen_no_map()
    ]).

gen_assoc() ->
    oneof([
        gen_child(),
        gen_few(gen_child()),
        {improper, gen_few(gen_child()), ?MARKER},
        gen_no_map(),
        gen_term()
    ]).

gen_child() ->
    ?LET(Schema, gen_hiding_schema(), gen_hiding_changeset(Schema, exactly(#{}))).

gen_params() ->
    oneof([
        gen_few(oneof([?MARKER, gen_term()])),
        {improper, gen_few(gen_term()), ?MARKER},
        ?MARKER,
        {?MARKER}
    ]).

gen_mode() -> oneof([plain, redacted, hidden, gen_term()]).

gen_fields() ->
    oneof([
        gen_few(oneof([id, email, token, password_hash])),
        {improper, gen_few(oneof([email, token])), token},
        gen_few(gen_term()),
        gen_term()
    ]).

gen_query() ->
    oneof([
        kura_query:from(?S),
        kura_query:where(kura_query:from(?S), {token, '=', ?MARKER}),
        kura_query:where(kura_query:from(?S), {email, '=', ~"a@b.c"}),
        kura_query:from(kura_test_bad_redact_schema),
        ?LET(Wheres, gen_term(), (kura_query:from(?S))#kura_query{wheres = ill_typed(Wheres)}),
        ?LET(From, gen_term(), #kura_query{from = ill_typed(From)}),
        gen_term()
    ]).

gen_error() ->
    ?LET(
        {Tag, Reason},
        {oneof([pgsql_error, gen_term()]), #{
            code => ~"22P02", message => ?MARKER, detail => [?MARKER], ?MARKER => ?MARKER
        }},
        oneof([Reason, {Tag, Reason}, {Tag, ?MARKER}, ?MARKER, [?MARKER]])
    ).

%%----------------------------------------------------------------------
%% PropEr instances are untyped; these hand them to the typed API.
%%----------------------------------------------------------------------

-spec ill_typed(term()) -> dynamic().
ill_typed(Term) -> Term.

shown_changeset(Changeset) -> kura_redact:changeset(ill_typed(Changeset)).

shown_record(Schema, Record) -> kura_redact:record(ill_typed(Schema), ill_typed(Record)).

shown(Mode, Params) -> kura_redact:shown(ill_typed(Mode), ill_typed(Params)).

sensitive_params(Params) -> kura_redact:sensitive_params(ill_typed(Params)).

query_params(Query, Params) -> kura_redact:query_params(ill_typed(Query), ill_typed(Params)).

write_params(Table, Fields, Params) ->
    kura_redact:write_params(ill_typed(Table), ill_typed(Fields), ill_typed(Params)).

update_all_params(Query, Fields, Params) ->
    kura_redact:update_all_params(ill_typed(Query), ill_typed(Fields), ill_typed(Params)).

mode(Query, Sources) -> kura_redact:mode(ill_typed(Query), ill_typed(Sources)).
