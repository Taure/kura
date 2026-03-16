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

total_pages_zero_entries_test() ->
    ?assertEqual(0, kura_paginator:total_pages(0, 10)).

total_pages_exact_fit_test() ->
    ?assertEqual(3, kura_paginator:total_pages(30, 10)).

total_pages_partial_last_page_test() ->
    ?assertEqual(4, kura_paginator:total_pages(31, 10)).

total_pages_single_entry_test() ->
    ?assertEqual(1, kura_paginator:total_pages(1, 20)).

total_pages_one_per_page_test() ->
    ?assertEqual(5, kura_paginator:total_pages(5, 1)).
