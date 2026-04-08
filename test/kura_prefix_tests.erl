-module(kura_prefix_tests).
-include_lib("eunit/include/eunit.hrl").

put_get_test() ->
    kura_prefix:put(<<"tenant_abc">>),
    ?assertEqual(<<"tenant_abc">>, kura_prefix:get()),
    kura_prefix:delete().

get_undefined_test() ->
    kura_prefix:delete(),
    ?assertEqual(undefined, kura_prefix:get()).

delete_test() ->
    kura_prefix:put(<<"tenant_abc">>),
    kura_prefix:delete(),
    ?assertEqual(undefined, kura_prefix:get()).

overwrite_test() ->
    kura_prefix:put(<<"tenant_1">>),
    kura_prefix:put(<<"tenant_2">>),
    ?assertEqual(<<"tenant_2">>, kura_prefix:get()),
    kura_prefix:delete().
