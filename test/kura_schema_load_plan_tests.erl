-module(kura_schema_load_plan_tests).
-moduledoc "kura_schema:load_plan/1 for a module that becomes a schema after it was first asked for.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(LATE, kura_test_late_schema).

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

a_module_that_is_not_a_schema_has_no_plan_test_() ->
    [
        ?_assertEqual(undefined, kura_schema:load_plan(lists)),
        ?_assertEqual(undefined, kura_schema:load_plan(lists)),
        ?_assertEqual(#{a => 1}, kura_db:load_row(lists, #{a => 1}))
    ].

a_schema_keeps_its_plan_test() ->
    Plan = kura_schema:load_plan(kura_test_schema),
    ?assertMatch({#{id := _}, _, _}, Plan),
    ?assertEqual(Plan, kura_schema:load_plan(kura_test_schema)).

load_schema(Module) ->
    Fields = [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = state, type = {enum, [open, closed]}}
    ],
    Exports = [
        erl_syntax:arity_qualifier(erl_syntax:atom(Name), erl_syntax:integer(0))
     || Name <- [table, fields]
    ],
    Forms = [
        erl_syntax:attribute(erl_syntax:atom(module), [erl_syntax:atom(Module)]),
        erl_syntax:attribute(erl_syntax:atom(export), [erl_syntax:list(Exports)]),
        constant(table, ~"late_items"),
        constant(fields, Fields)
    ],
    {ok, Module, Binary} = compile:forms([erl_syntax:revert(Form) || Form <- Forms]),
    {module, Module} = code:load_binary(Module, "kura_test_late_schema.erl", Binary),
    ok.

constant(Name, Value) ->
    erl_syntax:function(erl_syntax:atom(Name), [
        erl_syntax:clause([], none, [erl_syntax:abstract(Value)])
    ]).
