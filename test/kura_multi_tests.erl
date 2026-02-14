-module(kura_multi_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% new
%%----------------------------------------------------------------------

new_creates_empty_multi_test() ->
    M = kura_multi:new(),
    ?assertEqual([], kura_multi:to_list(M)).

%%----------------------------------------------------------------------
%% insert / update / delete / run
%%----------------------------------------------------------------------

insert_adds_operation_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"A">>}, [name]),
    M = kura_multi:insert(kura_multi:new(), create_user, CS),
    [{create_user, {insert, CS}}] = kura_multi:to_list(M).

update_adds_operation_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{id => 1}, #{name => <<"B">>}, [name]),
    M = kura_multi:update(kura_multi:new(), update_user, CS),
    [{update_user, {update, CS}}] = kura_multi:to_list(M).

delete_adds_operation_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{id => 1}, #{}, []),
    M = kura_multi:delete(kura_multi:new(), remove_user, CS),
    [{remove_user, {delete, CS}}] = kura_multi:to_list(M).

run_adds_operation_test() ->
    Fun = fun(_) -> {ok, done} end,
    M = kura_multi:run(kura_multi:new(), custom_step, Fun),
    [{custom_step, {run, Fun}}] = kura_multi:to_list(M).

insert_with_fun_test() ->
    Fun = fun(#{}) ->
        kura_changeset:cast(kura_test_schema, #{}, #{name => <<"A">>}, [name])
    end,
    M = kura_multi:insert(kura_multi:new(), create_user, Fun),
    [{create_user, {insert, Fun}}] = kura_multi:to_list(M).

%%----------------------------------------------------------------------
%% ordering
%%----------------------------------------------------------------------

operations_preserve_order_test() ->
    CS1 = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"A">>}, [name]),
    CS2 = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"B">>}, [name]),
    M = kura_multi:insert(
        kura_multi:insert(kura_multi:new(), step1, CS1),
        step2,
        CS2
    ),
    [{step1, _}, {step2, _}] = kura_multi:to_list(M).

%%----------------------------------------------------------------------
%% duplicate step name
%%----------------------------------------------------------------------

duplicate_name_raises_test() ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"A">>}, [name]),
    M = kura_multi:insert(kura_multi:new(), step1, CS),
    ?assertError({duplicate_step, step1}, kura_multi:insert(M, step1, CS)).

%%----------------------------------------------------------------------
%% append
%%----------------------------------------------------------------------

append_merges_multis_test() ->
    CS1 = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"A">>}, [name]),
    CS2 = kura_changeset:cast(kura_test_schema, #{}, #{name => <<"B">>}, [name]),
    M1 = kura_multi:insert(kura_multi:new(), step1, CS1),
    M2 = kura_multi:insert(kura_multi:new(), step2, CS2),
    M3 = kura_multi:append(M1, M2),
    Ops = kura_multi:to_list(M3),
    ?assertEqual(2, length(Ops)),
    [{step1, _}, {step2, _}] = Ops.
