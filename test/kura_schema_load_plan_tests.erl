-module(kura_schema_load_plan_tests).
-moduledoc "kura_schema:load_plan/1 for a module that becomes a schema after it was first asked for.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(LATE, kura_test_late_schema).
-define(LATE_SECRET, kura_test_late_secret_schema).

a_module_loaded_later_is_loaded_as_a_schema_test() ->
    Row = #{id => 1, state => ~"open"},
    try
        ?assertEqual(undefined, kura_schema:load_plan(?LATE)),
        ?assertEqual(Row, kura_db:load_row(?LATE, Row)),
        ok = load_schema(?LATE),
        ?assertMatch({#{state := {enum, [open, closed]}}, _, _}, kura_schema:load_plan(?LATE)),
        ?assertEqual(#{id => 1, state => open}, kura_db:load_row(?LATE, Row))
    after
        kura_schema:forget(?LATE),
        code:purge(?LATE),
        code:delete(?LATE)
    end.

a_module_loaded_later_is_a_schema_with_its_redacted_fields_test() ->
    Query = #kura_query{from = ?LATE_SECRET, wheres = [{state, '=', ~"open"}]},
    try
        ?assertNot(kura_schema:is_schema(?LATE_SECRET)),
        ?assertEqual([], kura_schema:redacted_fields(?LATE_SECRET)),
        ?assertEqual(plain, kura_redact:mode(Query)),
        ok = load_schema(?LATE_SECRET, [{"redact/0", "redact() -> [state]."}]),
        ?assert(kura_schema:is_schema(?LATE_SECRET)),
        ?assertEqual([secret, state], kura_schema:redacted_fields(?LATE_SECRET)),
        ?assertEqual(hidden, kura_redact:mode(Query))
    after
        kura_schema:forget(?LATE_SECRET),
        code:purge(?LATE_SECRET),
        code:delete(?LATE_SECRET)
    end.

%% Whatever is asked of the name first once the module is loaded, the
%% answer is the module's.
a_module_loaded_later_is_read_whatever_is_asked_first_test_() ->
    Asked = [
        {"redacted_fields", fun() -> kura_schema:redacted_fields(?LATE_SECRET) end, [], [
            secret, state
        ]},
        {"kura_redact:record", fun() -> kura_redact:record(?LATE_SECRET, #{state => ~"open"}) end,
            #{state => ~"open"}, #{state => ~"[redacted]"}},
        {"renamed_columns", fun() -> kura_schema:renamed_columns(?LATE_SECRET) end, #{}, #{
            label => ~"label_text"
        }},
        {"fields_by_column", fun() -> kura_schema:fields_by_column(?LATE_SECRET) end, #{}, #{
            ~"label_text" => label
        }},
        {"encrypted_fields", fun() -> kura_schema:encrypted_fields(?LATE_SECRET) end, [], [
            secret
        ]},
        {"encryption_specs", fun() -> maps:keys(kura_schema:encryption_specs(?LATE_SECRET)) end, [],
            [secret]}
    ],
    [
        {Name, fun() ->
            try
                [?assertEqual(Unloaded, Ask()) || {_, Ask, Unloaded, _} <- Asked],
                ok = load_schema(?LATE_SECRET, [{"redact/0", "redact() -> [state]."}]),
                ?assertEqual(Loaded, First())
            after
                kura_schema:forget(?LATE_SECRET),
                code:purge(?LATE_SECRET),
                code:delete(?LATE_SECRET)
            end
        end}
     || {Name, First, _, Loaded} <- Asked
    ].

%% What kura remembers of a module outlives the code that worked it out, so
%% each of these starts from nothing remembered.
a_loaded_module_that_is_not_a_schema_stays_one_test_() ->
    {setup, fun() -> kura_schema:forget(lists) end, [
        ?_assertNot(kura_schema:is_schema(lists)), ?_assertNot(kura_schema:is_schema(lists))
    ]}.

a_module_that_is_not_a_schema_has_no_plan_test_() ->
    {setup, fun() -> kura_schema:forget(lists) end, [
        ?_assertEqual(undefined, kura_schema:load_plan(lists)),
        ?_assertEqual(undefined, kura_schema:load_plan(lists)),
        ?_assertEqual(#{a => 1}, kura_db:load_row(lists, #{a => 1}))
    ]}.

a_schema_keeps_its_plan_test() ->
    kura_schema:forget(kura_test_schema),
    Plan = kura_schema:load_plan(kura_test_schema),
    ?assertMatch({#{id := _}, _, _}, Plan),
    ?assertEqual(Plan, kura_schema:load_plan(kura_test_schema)).

%% A plan another release of kura kept, or one that is not a plan, is
%% worked out again and kept in its place.
a_kept_plan_that_cannot_be_read_is_replaced_test_() ->
    Key = {kura_schema, load_plan, kura_test_schema},
    {setup,
        fun() ->
            kura_schema:forget(kura_test_schema),
            Plan = kura_schema:load_plan(kura_test_schema),
            {Plan, persistent_term:get(Key)}
        end,
        fun(_) -> kura_schema:forget(kura_test_schema) end, fun({Plan, Kept}) ->
            {Types, Specs, ByColumn} = Plan,
            [
                {Title, fun() ->
                    persistent_term:put(Key, Unreadable),
                    ?assertEqual(
                        #{id => 1, name => ~"n"},
                        kura_db:load_row(kura_test_schema, #{id => 1, name => ~"n"})
                    ),
                    ?assertEqual(Plan, kura_schema:load_plan(kura_test_schema)),
                    ?assertEqual(Kept, persistent_term:get(Key))
                end}
             || {Title, Unreadable} <- [
                    {"no version", Plan},
                    {"an older version", {0, Plan}},
                    {"a version that is not a number", {old, Plan}},
                    {"two parts", setelement(2, Kept, {Types, Specs})},
                    {"types that are not a map", setelement(2, Kept, {[], Specs, ByColumn})},
                    {"specs that are not a map", setelement(2, Kept, {Types, [], ByColumn})},
                    {"columns that are not a map", setelement(2, Kept, {Types, Specs, []})},
                    {"an atom", plan}
                ]
            ]
        end}.

a_kept_plan_carries_a_version_test() ->
    kura_schema:forget(kura_test_schema),
    Plan = kura_schema:load_plan(kura_test_schema),
    ?assertMatch(
        {Version, Plan} when is_integer(Version),
        persistent_term:get({kura_schema, load_plan, kura_test_schema})
    ).

load_schema(Module) ->
    load_schema(Module, []).

load_schema(Module, Callbacks) ->
    Fields = [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = state, type = {enum, [open, closed]}},
        #kura_field{name = label, type = string, column = ~"label_text"},
        #kura_field{name = secret, type = {encrypted, string}}
    ],
    Exports = lists:join(", ", ["table/0", "fields/0" | [Export || {Export, _} <- Callbacks]]),
    Forms = [
        form(io_lib:format("-module(~w).", [Module])),
        form(["-export([", Exports, "])."]),
        form("table() -> <<\"late_items\">>."),
        form(io_lib:format("fields() -> ~w.", [Fields]))
        | [form(Callback) || {_Export, Callback} <- Callbacks]
    ],
    {ok, Module, Binary} = compile:forms(Forms),
    {module, Module} = code:load_binary(Module, "kura_test_late_schema.erl", Binary),
    ok.

-spec form(io_lib:chars()) -> erl_parse:abstract_form().
form(Text) ->
    {ok, Tokens, _End} = erl_scan:string(binary_to_list(iolist_to_binary(Text))),
    {ok, Form} = erl_parse:parse_form(Tokens),
    Form.
