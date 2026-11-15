-module(kura_tokenizer_prop_tests).
-moduledoc """
Properties of the code that reads SQL text a caller wrote: lock clauses,
fragment placeholders, the table a raw statement names, and `like`
escaping. Each is checked against a model built from the generated parts,
and for any binary at all.
""".

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(NUMTESTS, 300).
-define(SELECT, "SELECT * FROM \"t\" ").

property_test_() ->
    {setup, fun() -> ok end,
        fun(_) ->
            application:unset_env(kura, strict_locks),
            kura_boot_warning:forget(lock_text)
        end,
        {timeout, 120, [
            {"a lock term compiles to text that reads as the term", fun() ->
                run(prop_lock_term_round_trip())
            end},
            {"lock text reads the same in any case and spacing", fun() ->
                run(prop_lock_text_is_read())
            end},
            {"any binary is a lock kura reads, or is refused or passed through whole", fun() ->
                run(prop_any_lock_text())
            end},
            {"only a fragment's placeholders are numbered", fun() ->
                run(prop_fragment_placeholders())
            end},
            {"a fragment's placeholders are counted against its parameters", fun() ->
                run(prop_fragment_mismatch())
            end},
            {"any binary is a fragment", fun() -> run(prop_any_fragment()) end},
            {"the table after the first keyword is the source", fun() ->
                run(prop_source_is_named())
            end},
            {"any binary has a source made of its own word characters, or none", fun() ->
                run(prop_any_source())
            end},
            {"an escaped pattern matches its text and nothing else", fun() ->
                run(prop_escape_like())
            end}
        ]}}.

like_model_test_() ->
    [
        ?_assert(like(~"a%", ~"abc")),
        ?_assert(like(~"a_c", ~"abc")),
        ?_assert(like(~"%", <<>>)),
        ?_assertNot(like(~"a_", ~"a")),
        ?_assertNot(like(~"a\\%", ~"abc")),
        ?_assert(like(~"a\\%", ~"a%")),
        ?_assertNot(like(<<"a", $\\>>, <<"a", $\\>>))
    ].

run(Prop) ->
    ?assert(proper:quickcheck(Prop, [?NUMTESTS, {to_file, user}])).

%%----------------------------------------------------------------------
%% PropEr instances are untyped; these hand them to the typed API.
%%----------------------------------------------------------------------

gen_some(AtLeast, Generator) ->
    ?LET(Count, range(AtLeast, 3), counted(Count, Generator)).

counted(Count, Generator) -> vector(Count, Generator).

bytes(Chars) -> list_to_binary(Chars).

utf8_text(Chars) -> unicode:characters_to_binary(Chars).

clauses_text(Clauses) -> iolist_to_binary([Text || {Text, _Clause} <- Clauses]).

%%----------------------------------------------------------------------
%% Locks
%%----------------------------------------------------------------------

prop_lock_term_round_trip() ->
    ?FORALL(Clauses, gen_lock_clauses(), lock_term_round_trips(Clauses)).

lock_term_round_trips(Clauses) ->
    Stored = kura_query:lock_clause(lock_term(Clauses)),
    Text = compiled_lock(Stored),
    Stored =:= stored_lock(Clauses) andalso
        kura_query:lock_clause(Stored) =:= Stored andalso
        kura_query:lock_clause(Text) =:= Stored.

prop_lock_text_is_read() ->
    ?FORALL(
        {Clauses, Strict},
        {gen_worded_clauses(), oneof([unset, false, true])},
        lock_text_is_read(Clauses, Strict)
    ).

lock_text_is_read(Clauses, Strict) ->
    Text = clauses_text(Clauses),
    lock_under(Strict, Text) =:= {ok, stored_lock([Clause || {_Text, Clause} <- Clauses])}.

prop_any_lock_text() ->
    ?FORALL(Text, gen_any_lock_text(), any_lock_text(Text)).

any_lock_text(Text) ->
    Refused = {error, {kura, {invalid_lock, Text}}},
    case [lock_under(Strict, Text) || Strict <- [unset, false, true]] of
        [{ok, {fragment, Text}}, {ok, {fragment, Text}}, Refused] ->
            true;
        [{ok, Stored}, {ok, Stored}, {ok, Stored}] ->
            kura_query:lock_clause(compiled_lock(Stored)) =:= Stored;
        _ ->
            false
    end.

lock_under(Strict, Text) ->
    case Strict of
        unset -> application:unset_env(kura, strict_locks);
        _ -> application:set_env(kura, strict_locks, Strict)
    end,
    try
        {ok, kura_query:lock_clause(Text)}
    catch
        error:Reason -> {error, Reason}
    end.

compiled_lock(Stored) ->
    {SQL, []} = kura_dialect_pg:to_sql(#kura_query{from = ~"t", lock = Stored}),
    <<?SELECT, Text/binary>> = iolist_to_binary(SQL),
    Text.

lock_term([Clause]) -> clause_term(Clause);
lock_term(Clauses) -> [clause_term(Clause) || Clause <- Clauses].

stored_lock(Clauses) -> lock_term(Clauses).

clause_term({Strength, [], undefined}) -> Strength;
clause_term({Strength, [], Wait}) -> {Strength, Wait};
clause_term({Strength, Tables, undefined}) -> {Strength, #{tables => Tables}};
clause_term({Strength, Tables, Wait}) -> {Strength, #{tables => Tables, wait => Wait}}.

gen_lock_clauses() ->
    gen_some(1, gen_lock_clause()).

gen_lock_clause() ->
    {gen_strength(), gen_some(0, gen_quoted_name()), gen_wait()}.

gen_strength() -> oneof([for_update, for_no_key_update, for_share, for_key_share]).

gen_wait() -> oneof([undefined, nowait, skip_locked]).

gen_quoted_name() ->
    ?LET(
        Chars,
        non_empty(
            list(
                oneof([
                    range($a, $z), range($A, $Z), range($0, $9), $_, $\s, $", $., $;, $', $-, 229
                ])
            )
        ),
        utf8_text(Chars)
    ).

%% A clause as someone would type it, beside the clause it should be read as.
gen_worded_clauses() ->
    gen_some(1, gen_worded_clause()).

gen_worded_clause() ->
    ?LET(
        {Strength, Tables, Wait, Gaps},
        {
            gen_strength(),
            gen_some(0, gen_worded_table()),
            gen_wait(),
            vector(12, gen_gap())
        },
        worded_clause(Strength, Tables, Wait, Gaps)
    ).

worded_clause(Strength, Tables, Wait, Gaps) ->
    Words =
        [~"FOR" | strength_words(Strength)] ++ of_words(Tables) ++ wait_words(Wait),
    {
        [
            [gen_cased_now(Word), Gap]
         || {Word, Gap} <- lists:zip(Words, lists:sublist(Gaps, length(Words)))
        ],
        {Strength, [Name || {_Text, Name} <- Tables], Wait}
    }.

%% Case is varied by position, so the text is a function of the instance.
gen_cased_now({as_is, Text}) ->
    Text;
gen_cased_now(Word) ->
    <<
        <<
            (case Index rem 2 of
                0 -> string:lowercase(<<Char>>);
                1 -> <<Char>>
            end)/binary
        >>
     || {Index, Char} <- lists:enumerate(binary_to_list(Word))
    >>.

strength_words(for_update) -> [~"UPDATE"];
strength_words(for_no_key_update) -> [~"NO", ~"KEY", ~"UPDATE"];
strength_words(for_share) -> [~"SHARE"];
strength_words(for_key_share) -> [~"KEY", ~"SHARE"].

of_words([]) ->
    [];
of_words(Tables) ->
    [~"OF" | lists:join({as_is, ~","}, [{as_is, Text} || {Text, _Name} <- Tables])].

wait_words(undefined) -> [];
wait_words(nowait) -> [~"NOWAIT"];
wait_words(skip_locked) -> [~"SKIP", ~"LOCKED"].

gen_gap() ->
    non_empty(list(oneof([$\s, $\t, $\n]))).

%% A table as written and the name it is read as: a bare name folds to
%% lower case, a quoted one is kept.
gen_worded_table() ->
    oneof([
        ?LET(Name, gen_quoted_name(), {quoted(Name), Name}),
        ?LET(
            {First, Rest},
            {
                oneof([range($a, $z), range($A, $Z), $_]),
                list(
                    oneof([range($a, $z), range($A, $Z), range($0, $9), $_])
                )
            },
            bare_table(First, Rest)
        )
    ]).

bare_table(First, Rest) ->
    Name = list_to_binary([First | Rest]),
    Folded = string:lowercase(Name),
    case lists:member(Folded, [~"nowait", ~"skip", ~"for", ~"of"]) of
        true -> {quoted(Folded), Folded};
        false -> {Name, Folded}
    end.

quoted(Name) ->
    <<$", (binary:replace(Name, ~"\"", ~"\"\"", [global]))/binary, $">>.

gen_any_lock_text() ->
    oneof([
        binary(),
        ?LET(
            {Clauses, Position, Byte, Edit},
            {gen_worded_clauses(), nat(), byte(), oneof([insert, delete, replace, keep])},
            edited(clauses_text(Clauses), Position, Byte, Edit)
        )
    ]).

edited(Text, _Position, _Byte, keep) ->
    Text;
edited(Text, Position, Byte, Edit) ->
    At = Position rem (byte_size(Text) + 1),
    <<Before:At/binary, After/binary>> = Text,
    case {Edit, After} of
        {insert, _} -> <<Before/binary, Byte, After/binary>>;
        {delete, <<_, Rest/binary>>} -> <<Before/binary, Rest/binary>>;
        {replace, <<_, Rest/binary>>} -> <<Before/binary, Byte, Rest/binary>>;
        {_, <<>>} -> Text
    end.

%%----------------------------------------------------------------------
%% Fragments
%%----------------------------------------------------------------------

prop_fragment_placeholders() ->
    ?FORALL(
        {Segments, Before},
        {non_empty(list(gen_segment())), range(0, 2)},
        placeholders_are_numbered(Segments, Before)
    ).

placeholders_are_numbered(Segments, Before) ->
    Count = length([placeholder || placeholder <- Segments]),
    Params = lists:seq(101, 100 + Count),
    {Texts, _Next} = lists:mapfoldl(fun numbered/2, Before + 1, Segments),
    Expected = iolist_to_binary(lists:join(~" ", Texts)),
    fragment_sql(written(Segments), Params, Before) =:=
        {ok, Expected, lists:seq(1, Before) ++ Params}.

numbered(placeholder, Counter) -> {[~"$", integer_to_binary(Counter)], Counter + 1};
numbered(Segment, Counter) -> {segment_text(Segment), Counter}.

written(Segments) ->
    iolist_to_binary(lists:join(~" ", [segment_text(Segment) || Segment <- Segments])).

prop_fragment_mismatch() ->
    ?FORALL(
        {Segments, Given},
        {non_empty(list(gen_segment())), range(0, 6)},
        mismatch_is_refused(Segments, Given)
    ).

mismatch_is_refused(Segments, Given) ->
    Count = length([placeholder || placeholder <- Segments]),
    Result = fragment_sql(written(Segments), lists:seq(1, Given), 0),
    case Given of
        Count -> element(1, Result) =:= ok;
        _ -> Result =:= {error, {kura, {fragment_placeholder_mismatch, Count, Given}}}
    end.

prop_any_fragment() ->
    ?FORALL(Text, binary(), any_fragment(Text)).

%% Whatever the text, it has some number of placeholders, and text with no
%% question mark in it is written as it is.
any_fragment(Text) ->
    case fragment_sql(Text, [], 0) of
        {ok, Text, []} ->
            true;
        {error, {kura, {fragment_placeholder_mismatch, Count, 0}}} when Count > 0 ->
            binary:match(Text, ~"?") =/= nomatch andalso
                element(1, fragment_sql(Text, lists:seq(1, Count), 0)) =:= ok;
        _ ->
            false
    end.

fragment_sql(Text, Params, Before) ->
    Conditions = [{id, N} || N <- lists:seq(1, Before)] ++ [{fragment, Text, Params}],
    Query = #kura_query{from = ~"t", wheres = Conditions},
    Leading = iolist_to_binary([
        [~"\"id\" = $", integer_to_binary(N), ~" AND "]
     || N <- lists:seq(1, Before)
    ]),
    try kura_dialect_pg:to_sql(Query) of
        {SQL, Bound} ->
            <<?SELECT, "WHERE ", Leading:(byte_size(Leading))/binary, Rewritten/binary>> =
                iolist_to_binary(SQL),
            {ok, Rewritten, Bound}
    catch
        error:Reason -> {error, Reason}
    end.

segment_text(placeholder) ->
    ~"?";
segment_text({plain, Text}) ->
    Text;
segment_text({literal, Body}) ->
    [$', binary:replace(Body, ~"'", ~"''", [global]), $'];
segment_text({escape_literal, Body}) ->
    [~"E'", <<<<(escaped_in_literal(Byte))/binary>> || <<Byte>> <= Body>>, $'];
segment_text({identifier, Body}) ->
    [$", binary:replace(Body, ~"\"", ~"\"\"", [global]), $"];
segment_text({line_comment, Body}) ->
    [~"--", Body, $\n];
segment_text({block_comment, Body, Nested}) ->
    [~"/*", Body, [[~"/*", Inner, ~"*/"] || Inner <- Nested], Body, ~"*/"];
segment_text({dollar_quoted, Tag, Body}) ->
    [$$, Tag, $$, Body, $$, Tag, $$].

escaped_in_literal(Byte) when Byte =:= $'; Byte =:= $\\ -> <<$\\, Byte>>;
escaped_in_literal(Byte) -> <<Byte>>.

gen_segment() ->
    oneof([
        placeholder,
        {plain, gen_text("abcxyzEe0189 =(),<>.:+")},
        {literal, gen_text("ab?\"-/*$\\ '")},
        {escape_literal, gen_text("ab?\"-/*$\\ '")},
        {identifier, gen_text("ab?'-/*$\\ \"")},
        {line_comment, gen_text("ab?'\"-/*$ ")},
        {block_comment, gen_text("ab?'\"-$ "), list(gen_text("ab?'\"-$ "))},
        {dollar_quoted, gen_dollar_tag(), gen_text("ab?'\"-/* \n")}
    ]).

gen_text(Alphabet) ->
    ?LET(Chars, list(oneof(Alphabet)), bytes(Chars)).

gen_dollar_tag() ->
    oneof([<<>>, ?LET(Chars, non_empty(list(oneof("tag_"))), bytes(Chars))]).

%%----------------------------------------------------------------------
%% The table a statement names
%%----------------------------------------------------------------------

prop_source_is_named() ->
    ?FORALL(
        {Filler, Keywords, Gap, Qualifier, Table, Rest},
        {
            list(
                oneof([~"SELECT", ~"a,", ~"b", ~"*", ~"(", ~"count(x)", ~"1", ~"fromage", ~"x_from"])
            ),
            oneof([
                [~"FROM"],
                [~"INTO"],
                [~"UPDATE"],
                [~"TABLE"],
                [~"TABLE", ~"IF", ~"EXISTS"],
                [~"TABLE", ~"IF", ~"NOT", ~"EXISTS"]
            ]),
            gen_gap(),
            oneof([none, gen_written_word()]),
            gen_written_word(),
            oneof([<<>>, ~" WHERE x = 1", ~"(a, b)", ~"\n", ~" AS f FROM other"])
        },
        source_is_named(Filler, Keywords, Gap, Qualifier, Table, Rest)
    ).

source_is_named(Filler, Keywords, Gap, Qualifier, {Written, Name}, Rest) ->
    Qualified =
        case Qualifier of
            none -> Written;
            {Schema, _} -> [Schema, $., Written]
        end,
    Statement = [
        [[Word, $\s] || Word <- Filler],
        [[gen_cased_now(Keyword), Gap] || Keyword <- Keywords],
        Qualified,
        Rest
    ],
    kura_db:extract_source(Statement) =:= Name andalso
        kura_db:extract_source(iolist_to_binary(Statement)) =:= Name.

existence_clause_test_() ->
    Source = fun kura_db:extract_source/1,
    [
        ?_assertEqual(~"t", Source(~"DROP TABLE IF EXISTS t")),
        ?_assertEqual(~"t", Source(~"CREATE TABLE IF NOT EXISTS t (id integer)")),
        ?_assertEqual(~"t", Source(~"ALTER TABLE IF EXISTS t ADD COLUMN c integer")),
        ?_assertEqual(~"t", Source(~"drop table if\n exists \"s\".\"t\" cascade")),
        ?_assertEqual(~"t", Source(~"CREATE TEMP TABLE IF NOT EXISTS\"t\" (id integer)")),
        ?_assertEqual(undefined, Source(~"DROP TABLE IF EXISTS")),
        {"a table may be named if, not or exists", [
            ?_assertEqual(~"if", Source(~"DROP TABLE if")),
            ?_assertEqual(~"if", Source(~"DROP TABLE if CASCADE")),
            ?_assertEqual(~"IF", Source(~"DROP TABLE IF NOT")),
            ?_assertEqual(~"if", Source(~"DROP TABLE IF EXISTS if")),
            ?_assertEqual(~"not", Source(~"DROP TABLE IF EXISTS not")),
            ?_assertEqual(~"exists", Source(~"CREATE TABLE IF NOT EXISTS exists (id integer)")),
            ?_assertEqual(~"IF", Source(~"DROP TABLE IF EXISTSX t"))
        ]},
        {"a statement that names no table has no source", [
            ?_assertEqual(undefined, Source(~"DROP INDEX IF EXISTS t_name_index")),
            ?_assertEqual(undefined, Source(~"DROP INDEX CONCURRENTLY IF EXISTS t_name_index"))
        ]}
    ].

gen_written_word() ->
    ?LET(
        {Chars, Quoted},
        {non_empty(list(oneof([range($a, $z), range($A, $Z), range($0, $9), $_]))), boolean()},
        written_word(bytes(Chars), Quoted)
    ).

written_word(Name, true) -> {<<$", Name/binary, $">>, Name};
written_word(Name, false) -> {Name, Name}.

prop_any_source() ->
    ?FORALL(
        Statement,
        oneof([binary(), gen_text("FROMfrom INTOUPDATEtable\"._\n\tx1")]),
        any_source(Statement)
    ).

any_source(Statement) ->
    case kura_db:extract_source(Statement) of
        undefined ->
            true;
        Name when is_binary(Name), Name =/= <<>> ->
            binary:match(Statement, Name) =/= nomatch andalso
                lists:all(fun is_word_byte/1, binary_to_list(Name))
    end.

is_word_byte(Byte) ->
    (Byte >= $a andalso Byte =< $z) orelse (Byte >= $A andalso Byte =< $Z) orelse
        (Byte >= $0 andalso Byte =< $9) orelse Byte =:= $_.

%%----------------------------------------------------------------------
%% like escaping
%%----------------------------------------------------------------------

prop_escape_like() ->
    ?FORALL(
        {Text, Other},
        {gen_like_text(), gen_like_text()},
        escaped_matches_only_itself(Text, Other)
    ).

escaped_matches_only_itself(Text, Other) ->
    Pattern = kura_query:escape_like(Text),
    Special = length([Byte || <<Byte>> <= Text, Byte =:= $% orelse Byte =:= $_ orelse Byte =:= $\\]),
    byte_size(Pattern) =:= byte_size(Text) + Special andalso
        like(Pattern, Text) andalso
        like(Pattern, Other) =:= (Other =:= Text) andalso
        like(<<$%, Pattern/binary, $%>>, <<Other/binary, Text/binary, Other/binary>>).

gen_like_text() ->
    oneof([binary(), gen_text("ab%_\\")]).

%% LIKE with a backslash as its escape character, over bytes.
like(<<>>, <<>>) ->
    true;
like(<<$\\, Byte, Pattern/binary>>, <<Byte, Text/binary>>) ->
    like(Pattern, Text);
like(<<$\\, _/binary>>, _Text) ->
    false;
like(<<$%, Pattern/binary>> = Whole, Text) ->
    like(Pattern, Text) orelse
        case Text of
            <<_, Rest/binary>> -> like(Whole, Rest);
            <<>> -> false
        end;
like(<<$_, Pattern/binary>>, <<_, Text/binary>>) ->
    like(Pattern, Text);
like(<<Byte, Pattern/binary>>, <<Byte, Text/binary>>) when Byte =/= $_ ->
    like(Pattern, Text);
like(_Pattern, _Text) ->
    false.
