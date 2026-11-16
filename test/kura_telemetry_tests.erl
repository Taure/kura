-module(kura_telemetry_tests).
-include_lib("eunit/include/eunit.hrl").

build_log_event_select_test() ->
    Result = #{command => select, rows => [#{id => 1}, #{id => 2}]},
    Event = kura_repo_worker:build_log_event(my_repo, <<"SELECT * FROM users">>, [], Result, 1500),
    ?assertEqual(<<"SELECT * FROM users">>, maps:get(query, Event)),
    ?assertEqual([], maps:get(params, Event)),
    ?assertEqual(ok, maps:get(result, Event)),
    ?assertEqual(2, maps:get(num_rows, Event)),
    ?assertEqual(1500, maps:get(duration_us, Event)),
    ?assertEqual(my_repo, maps:get(repo, Event)).

build_log_event_insert_test() ->
    Result = #{command => insert, num_rows => 1, rows => [#{id => 1}]},
    Event = kura_repo_worker:build_log_event(
        my_repo, <<"INSERT INTO users">>, [<<"Alice">>], Result, 500
    ),
    ?assertEqual(ok, maps:get(result, Event)),
    ?assertEqual(1, maps:get(num_rows, Event)),
    ?assertEqual([<<"Alice">>], maps:get(params, Event)).

build_log_event_error_test() ->
    Result = {error, #{code => <<"23505">>}},
    Event = kura_repo_worker:build_log_event(
        my_repo, <<"INSERT INTO users">>, [<<"Alice">>], Result, 200
    ),
    ?assertEqual(error, maps:get(result, Event)),
    ?assertEqual(0, maps:get(num_rows, Event)).

build_log_event_keys_test() ->
    Result = #{command => select, rows => []},
    Event = kura_repo_worker:build_log_event(my_repo, <<"SELECT 1">>, [], Result, 100),
    ExpectedKeys = lists:sort([query, params, result, num_rows, duration_us, repo]),
    ?assertEqual(ExpectedKeys, lists:sort(maps:keys(Event))).

default_logger_test() ->
    LogFun = kura_repo_worker:default_logger(),
    ?assert(is_function(LogFun, 1)).

%%----------------------------------------------------------------------
%% Telemetry metadata tests
%%----------------------------------------------------------------------

build_telemetry_metadata_select_test() ->
    Result = #{command => select, rows => [#{id => 1}]},
    Meta = kura_repo_worker:build_telemetry_metadata(
        my_repo, <<"SELECT * FROM \"users\" WHERE id = $1">>, [1], Result
    ),
    ?assertEqual(<<"SELECT * FROM \"users\" WHERE id = $1">>, maps:get(query, Meta)),
    ?assertEqual([1], maps:get(params, Meta)),
    ?assertEqual(my_repo, maps:get(repo, Meta)),
    ?assertEqual(ok, maps:get(result, Meta)),
    ?assertEqual(1, maps:get(num_rows, Meta)),
    ?assertEqual(<<"users">>, maps:get(source, Meta)),
    ?assertEqual(undefined, maps:get(tenant, Meta)),
    ?assertEqual(undefined, maps:get(error_reason, Meta)).

build_telemetry_metadata_insert_test() ->
    Result = #{command => insert, rows => [#{id => 1}]},
    Meta = kura_repo_worker:build_telemetry_metadata(
        my_repo, <<"INSERT INTO \"posts\" (title) VALUES ($1)">>, [<<"Hello">>], Result
    ),
    ?assertEqual(<<"posts">>, maps:get(source, Meta)),
    ?assertEqual(ok, maps:get(result, Meta)),
    ?assertEqual(undefined, maps:get(error_reason, Meta)).

build_telemetry_metadata_update_test() ->
    Result = #{command => update, rows => [#{id => 1}]},
    Meta = kura_repo_worker:build_telemetry_metadata(
        my_repo, <<"UPDATE \"users\" SET name = $1">>, [<<"Bob">>], Result
    ),
    ?assertEqual(<<"users">>, maps:get(source, Meta)).

build_telemetry_metadata_error_test() ->
    Result = {error, #{code => <<"23505">>}},
    Meta = kura_repo_worker:build_telemetry_metadata(
        my_repo, <<"INSERT INTO \"users\"">>, [], Result
    ),
    ?assertEqual(error, maps:get(result, Meta)),
    ?assertEqual(0, maps:get(num_rows, Meta)),
    ?assertEqual(#{code => <<"23505">>}, maps:get(error_reason, Meta)).

%% Bytes just outside the letters, the digits and the ASCII range.
extract_source_at_the_edge_of_a_name_test_() ->
    Named = [
        <<127, "FROM t">>,
        <<"x", 128, " FROM t">>,
        ~"x[FROM t",
        ~"x{FROM t",
        ~"x:FROM t",
        ~"9:FROM t",
        <<"x", 127, "FROM t">>,
        ~"FROM t[",
        ~"FROM t{",
        ~"FROM t:x",
        <<"FROM t", 127>>,
        <<"$a", 128, "$ FROM hidden $a", 128, "$ FROM t">>,
        <<"$a", 129, "$ FROM hidden $a", 129, "$ FROM t">>,
        <<"$a", 127, "$ FROM t">>,
        ~"x$a$ FROM t $a$",
        <<"x", 128, "$a$ FROM t $a$">>,
        <<"x", 129, "$a$ FROM t $a$">>,
        <<"x", 127, "$a$ FROM hidden $a$ FROM t">>
    ],
    Unnamed = [
        <<128, "FROM t">>,
        <<129, "FROM t">>,
        <<"x", 128, "FROM t">>,
        <<"x", 129, "FROM t">>,
        <<"FROM t", 128>>,
        <<"FROM t", 129>>,
        ~"FROM t$x"
    ],
    [?_assertEqual({SQL, ~"t"}, {SQL, kura_db:extract_source(SQL)}) || SQL <- Named] ++
        [?_assertEqual({SQL, undefined}, {SQL, kura_db:extract_source(SQL)}) || SQL <- Unnamed] ++
        [
            ?_assertEqual(~"T", kura_db:extract_source(~"FROM T[")),
            ?_assertEqual(~"T", kura_db:extract_source(~"FROM T^")),
            ?_assertEqual(~"t9", kura_db:extract_source(~"FROM t9:"))
        ].

build_telemetry_metadata_result_test_() ->
    Reported = fun(Result) ->
        maps:with(
            [result, num_rows, error_reason],
            kura_repo_worker:build_telemetry_metadata(my_repo, ~"SELECT 1", [], Result)
        )
    end,
    Ok = fun(Rows) -> #{result => ok, num_rows => Rows, error_reason => undefined} end,
    [
        ?_assertEqual(Ok(4), Reported(#{command => delete, num_rows => 4})),
        ?_assertEqual(Ok(1), Reported(#{command => select, rows => [#{id => 1}]})),
        {"a count that is no number is not reported",
            ?_assertEqual(Ok(1), Reported(#{num_rows => many, rows => [#{id => 1}]}))},
        {"a result that is neither rows nor a count", [
            ?_assertEqual(Ok(0), Reported(#{command => listen})),
            ?_assertEqual(Ok(0), Reported(#{rows => none})),
            ?_assertEqual(Ok(0), Reported(ok))
        ]}
    ].

build_telemetry_metadata_rows_affected_test_() ->
    Counted = fun(Result) ->
        maps:get(
            num_rows,
            kura_repo_worker:build_telemetry_metadata(my_repo, ~"SELECT 1", [], Result)
        )
    end,
    [
        {"a write that returns no rows counts the rows it affected", [
            ?_assertEqual(3, Counted(#{command => insert, num_rows => 3, rows => []})),
            ?_assertEqual(2, Counted(#{command => update, num_rows => 2, rows => []})),
            ?_assertEqual(4, Counted(#{command => delete, num_rows => 4}))
        ]},
        ?_assertEqual(
            2, Counted(#{command => insert, num_rows => 2, rows => [#{id => 1}, #{id => 2}]})
        ),
        ?_assertEqual(1, Counted(#{command => select, rows => [#{id => 1}]})),
        ?_assertEqual(0, Counted(#{command => select, rows => []})),
        ?_assertEqual(0, Counted({error, timeout}))
    ].

build_telemetry_metadata_no_source_test() ->
    Result = #{command => select, rows => []},
    Meta = kura_repo_worker:build_telemetry_metadata(
        my_repo, <<"SELECT 1">>, [], Result
    ),
    ?assertEqual(undefined, maps:get(source, Meta)).

extract_source_test() ->
    ?assertEqual(<<"users">>, kura_repo_worker:extract_source(<<"SELECT * FROM \"users\"">>)),
    ?assertEqual(<<"posts">>, kura_repo_worker:extract_source(<<"INSERT INTO \"posts\"">>)),
    ?assertEqual(<<"users">>, kura_repo_worker:extract_source(<<"UPDATE \"users\" SET">>)),
    ?assertEqual(<<"t">>, kura_repo_worker:extract_source(<<"CREATE TABLE \"t\"">>)),
    ?assertEqual(undefined, kura_repo_worker:extract_source(<<"SELECT 1">>)).

extract_source_without_a_regex_test_() ->
    Source = fun kura_db:extract_source/1,
    [
        ?_assertEqual(~"users", Source(~"select * from users where id = 1")),
        ?_assertEqual(~"users", Source(~"DELETE FrOm\n\t \"users\" WHERE id = $1")),
        ?_assertEqual(~"users", Source([~"UPDATE ", [~"\"users\""], ~" SET x = 1"])),
        ?_assertEqual(~"t_1", Source(~"ALTER TABLE t_1 ADD COLUMN c int")),
        {"the first keyword followed by a name wins",
            ?_assertEqual(~"b", Source(~"SELECT x FROM (SELECT 1) s JOIN a ON TRUE, INTO b"))},
        {"a keyword inside a longer word is not one", [
            ?_assertEqual(~"t", Source(~"SELECT valid_from AS f FROM t")),
            ?_assertEqual(~"t", Source(~"SELECT wherefrom x, from_date y FROM t")),
            ?_assertEqual(undefined, Source(~"SELECT wherefrom x, updates y"))
        ]},
        ?_assertEqual(undefined, Source(~"SELECT \"from\", \"table\"")),
        ?_assertEqual(undefined, Source(~"SELECT 1 FROM")),
        ?_assertEqual(undefined, Source(~"SELECT 1 FROM \"\"")),
        ?_assertEqual(undefined, Source(~"")),
        {"a schema-qualified table is named by its table", [
            ?_assertEqual(~"users", Source(~"SELECT * FROM \"tenant_a\".\"users\"")),
            ?_assertEqual(~"users", Source(~"INSERT INTO tenant_a.users (id) VALUES ($1)")),
            ?_assertEqual(~"tenant_a", Source(~"SELECT * FROM tenant_a.(x)"))
        ]}
    ].

extract_source_reads_no_keyword_in_quoted_text_test_() ->
    Source = fun kura_db:extract_source/1,
    [
        {"a string literal", [
            ?_assertEqual(~"t", Source(~"SELECT 'x from hidden' FROM t")),
            ?_assertEqual(~"t", Source(~"SELECT 'it''s from hidden' FROM t")),
            ?_assertEqual(~"t", Source(~"SELECT N'from hidden', 'a' 'into hidden' FROM t")),
            ?_assertEqual(undefined, Source(~"SELECT 'x from hidden"))
        ]},
        {"an escape string", [
            ?_assertEqual(~"t", Source(~"SELECT E'it\\'s from hidden' FROM t")),
            ?_assertEqual(~"t", Source(~"SELECT e'\\\\' FROM t")),
            ?_assertEqual(~"t", Source(~"SELECT E'a''\\\\\\' from hidden' FROM t")),
            {"a backslash escapes nothing in a plain literal",
                ?_assertEqual(~"t", Source(~"SELECT '\\' FROM t"))},
            {"nor after a word that only ends in e",
                ?_assertEqual(~"t", Source(~"SELECT name'\\' FROM t"))}
        ]},
        {"a quoted identifier", [
            ?_assertEqual(~"t", Source(~"SELECT \"x from hidden\" FROM t")),
            ?_assertEqual(~"t", Source(~"SELECT \"a\"\" from hidden\" FROM t")),
            ?_assertEqual(undefined, Source(~"SELECT \"x from hidden"))
        ]},
        {"a line comment", [
            ?_assertEqual(~"t", Source(~"-- update hidden\nSELECT 1 FROM t")),
            ?_assertEqual(~"t", Source(~"SELECT 1 FROM -- from hidden\n t")),
            ?_assertEqual(undefined, Source(~"SELECT 1 -- from hidden"))
        ]},
        {"a block comment, nested as PostgreSQL nests them", [
            ?_assertEqual(~"users", Source(~"/* from audit_log */ SELECT * FROM users")),
            ?_assertEqual(~"t", Source(~"/* a /* from hidden */ into hidden */ SELECT 1 FROM t")),
            ?_assertEqual(~"t", Source(~"SELECT 1 FROM/* from hidden */t")),
            ?_assertEqual(~"t", Source(~"DROP TABLE IF /* a */ EXISTS /* b */ t")),
            ?_assertEqual(undefined, Source(~"SELECT 1 /* from hidden")),
            ?_assertEqual(undefined, Source(~"SELECT 1 /* /* */ from hidden"))
        ]},
        {"a dollar-quoted string", [
            ?_assertEqual(~"t", Source(~"SELECT $q$ from hidden $q$ FROM t")),
            ?_assertEqual(~"t", Source(~"SELECT $$ from hidden $$ FROM t")),
            ?_assertEqual(~"t", Source(~"SELECT $q$ $$ from hidden $x$ $q$ FROM t")),
            ?_assertEqual(~"t", Source(~"SELECT $q1$ from hidden $q1$ FROM t")),
            ?_assertEqual(undefined, Source(~"SELECT $q$ from hidden $$ FROM t")),
            {"a parameter opens none",
                ?_assertEqual(~"t", Source(~"SELECT $1 FROM t WHERE a = $2"))},
            {"nor does a dollar inside a name",
                ?_assertEqual(~"t", Source(~"SELECT a$b$ FROM t WHERE c$b$ = 1"))},
            {"nor a dollar that no tag follows",
                ?_assertEqual(~"t", Source(~"SELECT $ a, $b FROM t"))}
        ]},
        {"a column named as a keyword",
            ?_assertEqual(~"u", Source(~"SELECT t.from, t.\"into\", t . update FROM u"))}
    ].

extract_source_steps_over_only_test_() ->
    Source = fun kura_db:extract_source/1,
    [
        [
            ?_assertEqual(~"t", Source(SQL))
         || SQL <- [
                ~"SELECT * FROM ONLY t",
                ~"select * from only\n\"s\".\"t\" x",
                ~"DELETE FROM ONLY /* a */ t",
                ~"UPDATE ONLY t SET a = 1",
                ~"ALTER TABLE ONLY t ADD COLUMN c int",
                ~"ALTER TABLE IF EXISTS ONLY t ADD COLUMN c int",
                ~"TRUNCATE TABLE oNlY t"
            ]
        ],
        {"a longer word is a name", [
            ?_assertEqual(~"only_these", Source(~"SELECT * FROM only_these")),
            ?_assertEqual(~"onlyx", Source(~"SELECT * FROM onlyx t"))
        ]},
        {"it is never the name", [
            ?_assertEqual(undefined, Source(~"SELECT * FROM ONLY")),
            ?_assertEqual(undefined, Source(~"SELECT * FROM ONLY (t)"))
        ]},
        {"INTO takes none", ?_assertEqual(~"only", Source(~"INSERT INTO only t"))}
    ].

extract_source_of_a_name_it_cannot_read_test_() ->
    Source = fun kura_db:extract_source/1,
    [
        {"a qualifier is passed over whatever it holds", [
            ?_assertEqual(~"t", Source(~"SELECT * FROM \"we\"\"ird\".\"t\"")),
            ?_assertEqual(~"t", Source(~"SELECT * FROM \"my schema\".t")),
            ?_assertEqual(~"t", Source(~"SELECT * FROM db.s.t")),
            ?_assertEqual(~"t", Source(~"SELECT * FROM s . t x")),
            ?_assertEqual(~"t", Source(~"SELECT * FROM \"my schema\" /* a */ .\n\"t\"")),
            ?_assertEqual(~"t", Source(~"SELECT * FROM \"db\".s$1.\"t\" x"))
        ]},
        {"a name with more than letters, digits and underscores is none", [
            ?_assertEqual(undefined, Source(SQL))
         || SQL <- [
                ~"SELECT * FROM \"my table\"",
                ~"SELECT * FROM \"ta\"\"ble\"",
                ~"SELECT * FROM \"\"\"table\"",
                ~"SELECT * FROM s.\"my table\"",
                ~"SELECT * FROM \"s\".\"t\"\"x\"",
                ~"SELECT * FROM \"my.table\"",
                ~"SELECT * FROM a$b",
                ~"SELECT * FROM \"a$b\"",
                <<"SELECT * FROM tabl", 195, 169>>,
                <<"SELECT * FROM ", 195, 169, "t x">>,
                ~"SELECT * FROM \"never closed",
                ~"SELECT * FROM \"my table\".\"\""
            ]
        ]},
        {"and no later table stands in for it",
            ?_assertEqual(undefined, Source(~"SELECT * FROM \"my table\" JOIN t ON TRUE, INTO u"))}
    ].

extract_source_reads_the_start_of_a_statement_test_() ->
    Source = fun kura_db:extract_source/1,
    Long = binary:copy(~"a, ", 3000),
    Named = fun(Size) -> <<"SELECT 1 FROM ", (binary:copy(~"n", Size - 14))/binary>> end,
    [
        {"a table named in the first 8192 bytes",
            ?_assertEqual(~"t", Source([~"SELECT 1 FROM t WHERE x IN (", Long, ~")"]))},
        {"one named after them is none",
            ?_assertEqual(undefined, Source([~"SELECT ", Long, ~"1 FROM t"]))},
        {"a name that ends before them", [
            ?_assertEqual(binary:copy(~"n", 8178), Source(Named(8192))),
            ?_assertEqual(binary:copy(~"n", 8176), Source([Named(8190), ~" x"])),
            ?_assertEqual(binary:copy(~"n", 8176), Source([Named(8190), ~" JOIN u ON TRUE"]))
        ]},
        {"a name they may have cut short is none", [
            ?_assertEqual(undefined, Source([Named(8192), ~"n"])),
            ?_assertEqual(undefined, Source([Named(8192), ~" x"])),
            ?_assertEqual(undefined, Source([Named(8191), ~" .x"])),
            ?_assertEqual(undefined, Source([Named(8192), binary:copy(~"n", 9000)])),
            ?_assertEqual(undefined, Source([~"SELECT 1 FROM s.", binary:copy(~"n", 8176), ~"n"])),
            ?_assertEqual(undefined, Source([binary:copy(~" ", 8187), ~"FROM t"])),
            ?_assertEqual(undefined, Source([binary:copy(~" ", 8186), ~"FROM s.t"]))
        ]},
        {"megabytes are not read", fun() ->
            Huge = binary:copy(~"x from' ", 1 bsl 19),
            ?assertEqual(undefined, Source(Huge)),
            ?assert(reductions(fun() -> Source(Huge) end) < 100000)
        end}
    ].

extract_source_of_what_is_no_text_test_() ->
    Source = fun(SQL) -> kura_db:extract_source(kura_test_typing:ill_typed(SQL)) end,
    [
        ?_assertEqual(undefined, Source(from_t)),
        ?_assertEqual(undefined, Source(<<1:3>>)),
        ?_assertEqual(undefined, Source([~"SELECT 1 FROM t" | kura_test_typing:ill_typed(from_u)])),
        ?_assertEqual(undefined, Source([~"SELECT 1 FROM t", 256])),
        ?_assertEqual(undefined, Source({~"SELECT 1 FROM t"}))
    ].

reductions(Run) ->
    {reductions, Before} = erlang:process_info(self(), reductions),
    _ = Run(),
    {reductions, After} = erlang:process_info(self(), reductions),
    After - Before.

extract_source_reads_keywords_in_any_case_test_() ->
    [
        ?_assertEqual(~"t", kura_db:extract_source(SQL))
     || SQL <- [
            ~"insert into t (a) values (1)",
            ~"InSeRt InTo\tt (a) values (1)",
            ~"update t set a = 1",
            ~"uPdAtE\nt set a = 1",
            ~"delete from t",
            ~"select 1 from\rt",
            ~"create table t (a int)",
            ~"alter TaBlE\tt add column c int",
            ~"drop table if exists t",
            ~"DROP TABLE iF\teXiStS\nt",
            ~"create table if not exists t (a int)",
            ~"CREATE TABLE If\rNoT\tExIsTs\nt (a int)"
        ]
    ].

extract_source_reads_whole_words_test_() ->
    Source = fun kura_db:extract_source/1,
    [
        {"a keyword at the end of a longer word", [
            ?_assertEqual(~"t", Source(~"SELECT Xfrom a FROM t")),
            ?_assertEqual(~"t", Source(~"SELECT 1from a FROM t")),
            ?_assertEqual(~"t", Source(~"SELECT _from a FROM t")),
            ?_assertEqual(~"t", Source(~"SELECT aFROM b FROM t"))
        ]},
        {"a keyword at the start of a longer word", [
            ?_assertEqual(~"t", Source(~"SELECT intox a, tablex b FROM t"))
        ]},
        {"IF, NOT and EXISTS as part of a name", [
            ?_assertEqual(~"ifxexists", Source(~"CREATE TABLE ifxexists (a int)")),
            ?_assertEqual(~"IF", Source(~"DROP TABLE IF notxexists")),
            ?_assertEqual(~"IF", Source(~"DROP TABLE IF EXISTSed t")),
            ?_assertEqual(~"IF", Source(~"DROP TABLE IF EXISTS1 t")),
            ?_assertEqual(~"IF", Source(~"DROP TABLE IF EXISTS_x t"))
        ]}
    ].
