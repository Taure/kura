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

%%----------------------------------------------------------------------
%% Size validation: none of these reach the repo, so no_such_repo is safe
%%----------------------------------------------------------------------

invalid_page_test_() ->
    [
        ?_assertEqual(
            {error, {invalid_page, Page}},
            kura_paginator:paginate(no_such_repo, kura_query:from(user), #{page => Page})
        )
     || Page <- [0, -1, 1.5, ~"2", undefined]
    ].

invalid_page_size_test_() ->
    [
        ?_assertEqual(
            {error, {invalid_page_size, Size}},
            kura_paginator:paginate(no_such_repo, kura_query:from(user), #{page_size => Size})
        )
     || Size <- [0, -20, 2.0, ~"20", undefined]
    ].

invalid_limit_test_() ->
    [
        ?_assertEqual(
            {error, {invalid_limit, Limit}},
            kura_paginator:cursor_paginate(no_such_repo, kura_query:from(user), #{limit => Limit})
        )
     || Limit <- [0, -1, 10.0, ~"10", undefined]
    ].

max_page_size_option_refuses_a_larger_size_test() ->
    Q = kura_query:from(user),
    ?assertEqual(
        {error, {page_size_too_large, 51, 50}},
        kura_paginator:paginate(no_such_repo, Q, #{page_size => 51, max_page_size => 50})
    ),
    ?assertEqual(
        {error, {page_size_too_large, 51, 50}},
        kura_paginator:cursor_paginate(no_such_repo, Q, #{limit => 51, max_page_size => 50})
    ).

max_page_size_setting_refuses_a_larger_size_test_() ->
    {setup, fun() -> application:set_env(kura, max_page_size, 100) end,
        fun(_) -> application:unset_env(kura, max_page_size) end, [
            ?_assertEqual(
                {error, {page_size_too_large, 101, 100}},
                kura_paginator:paginate(no_such_repo, kura_query:from(user), #{page_size => 101})
            ),
            ?_assertEqual(
                {error, {page_size_too_large, 101, 100}},
                kura_paginator:cursor_paginate(no_such_repo, kura_query:from(user), #{
                    limit => 101
                })
            ),
            ?_assertEqual(
                {error, {page_size_too_large, 101, 60}},
                kura_paginator:paginate(no_such_repo, kura_query:from(user), #{
                    page_size => 101, max_page_size => 60
                })
            )
        ]}.

invalid_max_page_size_setting_raises_test_() ->
    {setup, fun() -> application:set_env(kura, max_page_size, 0) end,
        fun(_) -> application:unset_env(kura, max_page_size) end, [
            ?_assertError(
                {kura, {invalid_setting, max_page_size, 0}},
                kura_paginator:paginate(no_such_repo, kura_query:from(user), #{})
            )
        ]}.

invalid_max_page_size_raises_test_() ->
    [
        ?_assertError(
            {kura, {invalid_setting, max_page_size, Max}},
            kura_paginator:paginate(no_such_repo, kura_query:from(user), #{max_page_size => Max})
        )
     || Max <- [0, -1, ~"100", 1.5]
    ].
