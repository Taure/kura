-module(kura_paginator_edge_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

cursor_both_after_and_before_raises_test() ->
    ?assertError(
        badarg,
        kura_paginator:cursor_paginate(
            unused_repo,
            #kura_query{},
            #{'after' => 1, before => 10}
        )
    ).
