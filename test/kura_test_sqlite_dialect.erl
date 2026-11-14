-module(kura_test_sqlite_dialect).
-moduledoc """
The kura_sqlite dialect's strategy, for tests in kura: kura_dialect_pg's
SQL with each `$N` placeholder rewritten to SQLite's `?N`. As there, a `$`
inside a string literal, a quoted identifier, a comment or an identifier,
and one not followed by a digit, is text.
""".

-export([to_sql/1, delete_all/1, insert_all/3, placeholder/1]).

to_sql(Query) ->
    swapped(kura_dialect_pg:to_sql(Query)).

delete_all(Query) ->
    swapped(kura_dialect_pg:delete_all(Query)).

insert_all(Table, Fields, Rows) ->
    swapped(kura_dialect_pg:insert_all(Table, Fields, Rows)).

placeholder(N) ->
    [~"?", integer_to_binary(N)].

swapped({SQL, Params}) ->
    {statement_text(iolist_to_binary(SQL), $\s, <<>>), Params}.

statement_text(<<>>, _Previous, Acc) ->
    Acc;
statement_text(<<$$, Digit, Rest/binary>>, Previous, Acc) when Digit >= $0, Digit =< $9 ->
    case identifier_byte(Previous) of
        true -> statement_text(Rest, Digit, <<Acc/binary, $$, Digit>>);
        false -> statement_text(Rest, Digit, <<Acc/binary, $?, Digit>>)
    end;
statement_text(<<Quote, Rest/binary>>, _Previous, Acc) when Quote =:= $'; Quote =:= $" ->
    {Quoted, Tail} = quoted(Rest, Quote, <<Quote>>),
    statement_text(Tail, Quote, <<Acc/binary, Quoted/binary>>);
statement_text(<<"--", Rest/binary>>, _Previous, Acc) ->
    {Skipped, Tail} = through(Rest, ~"\n"),
    statement_text(Tail, $\n, <<Acc/binary, "--", Skipped/binary>>);
statement_text(<<"/*", Rest/binary>>, _Previous, Acc) ->
    {Skipped, Tail} = through(Rest, ~"*/"),
    statement_text(Tail, $/, <<Acc/binary, "/*", Skipped/binary>>);
statement_text(<<Byte, Rest/binary>>, _Previous, Acc) ->
    statement_text(Rest, Byte, <<Acc/binary, Byte>>).

quoted(<<Quote, Quote, Rest/binary>>, Quote, Acc) ->
    quoted(Rest, Quote, <<Acc/binary, Quote, Quote>>);
quoted(<<Quote, Rest/binary>>, Quote, Acc) ->
    {<<Acc/binary, Quote>>, Rest};
quoted(<<Byte, Rest/binary>>, Quote, Acc) ->
    quoted(Rest, Quote, <<Acc/binary, Byte>>);
quoted(<<>>, _Quote, Acc) ->
    {Acc, <<>>}.

through(Text, End) ->
    case binary:split(Text, End) of
        [Skipped, Tail] -> {<<Skipped/binary, End/binary>>, Tail};
        [Unterminated] -> {Unterminated, <<>>}
    end.

identifier_byte(Byte) ->
    (Byte >= $a andalso Byte =< $z) orelse (Byte >= $A andalso Byte =< $Z) orelse
        (Byte >= $0 andalso Byte =< $9) orelse Byte =:= $_ orelse Byte =:= $$ orelse Byte >= 128.
