-module(kura_like_escape_SUITE).
-moduledoc """
What PostgreSQL makes of a pattern from `kura_query:escape_like/1`: it
matches the text it was made from and no other, under `LIKE` and `ILIKE`.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    an_escaped_pattern_matches_its_text/1,
    an_escaped_pattern_matches_nothing_else/1,
    an_unescaped_pattern_would_match/1,
    an_escaped_pattern_between_wildcards_finds_the_text/1
]).

-define(REPO, kura_like_escape_test_repo).

all() ->
    [
        an_escaped_pattern_matches_its_text,
        an_escaped_pattern_matches_nothing_else,
        an_unescaped_pattern_would_match,
        an_escaped_pattern_between_wildcards_finds_the_text
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(minato),
    {ok, _} = application:ensure_all_started(kura),
    application:set_env(kura, ensure_database, false),
    application:set_env(kura, ?REPO, #{
        pool => ?REPO,
        dialect => kura_dialect_pg,
        pool_module => kura_pool_minato,
        driver_module => kura_driver_minato,
        database => ~"kura_test",
        hostname => ~"localhost",
        port => 5555,
        username => ~"postgres",
        password => ~"root",
        pool_size => 2
    }),
    ok = kura_repo_worker:start(?REPO),
    Config.

end_per_suite(_Config) ->
    application:unset_env(kura, ?REPO),
    application:unset_env(kura, ensure_database),
    ok.

%% Each text beside one an unescaped pattern of it would also match.
samples() ->
    [
        {~"100%", ~"100 percent"},
        {~"a_c", ~"abc"},
        {~"%", ~"anything"},
        {~"_", ~"x"},
        {~"50%_off", ~"50 per cent, 1 off"},
        {~"%%", <<>>},
        {~"a%b_c%", ~"a--bxc"},
        {~"snö_%", ~"snöre"}
    ].

%% Texts with nothing an unescaped pattern would read as a wildcard.
plain_samples() ->
    [
        ~"plain",
        <<>>,
        ~"C:\\temp\\x",
        <<$\\>>,
        <<$\\, $\\>>,
        ~"it's \"quoted\"",
        ~"räksmörgås"
    ].

matches(Operator, Text, Pattern) ->
    {ok, [#{matched := Matched}]} = kura_repo_worker:query(
        ?REPO, [~"SELECT $1::text ", Operator, ~" $2::text AS matched"], [Text, Pattern]
    ),
    Matched.

an_escaped_pattern_matches_its_text(_Config) ->
    [
        ?assert(matches(Operator, Text, kura_query:escape_like(Text)), {Operator, Text})
     || Operator <- [~"LIKE", ~"ILIKE"],
        Text <- plain_samples() ++ [Text || {Text, _Other} <- samples()]
    ].

an_escaped_pattern_matches_nothing_else(_Config) ->
    [
        ?assertNot(matches(Operator, Other, kura_query:escape_like(Text)), {Operator, Text})
     || Operator <- [~"LIKE", ~"ILIKE"], {Text, Other} <- samples()
    ].

an_unescaped_pattern_would_match(_Config) ->
    [?assert(matches(~"LIKE", Other, Text), Text) || {Text, Other} <- samples()].

an_escaped_pattern_between_wildcards_finds_the_text(_Config) ->
    [
        begin
            Pattern = <<$%, (kura_query:escape_like(Text))/binary, $%>>,
            ?assert(matches(~"LIKE", <<"before ", Text/binary, " after">>, Pattern), Text),
            ?assertNot(matches(~"LIKE", <<"before ", Other/binary, " after">>, Pattern), Text)
        end
     || {Text, Other} <- samples(), Text =/= ~"%", Text =/= ~"%%", Text =/= ~"_"
    ].
