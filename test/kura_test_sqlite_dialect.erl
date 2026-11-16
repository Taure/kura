-module(kura_test_sqlite_dialect).
-moduledoc """
The kura_sqlite dialect's strategy, for tests in kura: kura_dialect_pg's
SQL with each `$N` placeholder rewritten to SQLite's `?N`. As there, a `$`
inside a string literal, a quoted identifier, a comment or an identifier,
and one not followed by a digit, is text. `load_stored/2` reads the forms
SQLite stores a boolean, a date, a time and a datetime in.
""".

-export([to_sql/1, delete_all/1, insert_all/3, placeholder/1, load_stored/2]).

-define(IS_TEMPORAL(Type),
    (Type =:= date orelse Type =:= time orelse Type =:= utc_datetime orelse
        Type =:= naive_datetime)
).

to_sql(Query) ->
    swapped(kura_dialect_pg:to_sql(Query)).

delete_all(Query) ->
    swapped(kura_dialect_pg:delete_all(Query)).

insert_all(Table, Fields, Rows) ->
    swapped(kura_dialect_pg:insert_all(Table, Fields, Rows)).

placeholder(N) ->
    [~"?", integer_to_binary(N)].

%% SQLite has no boolean, date or time type: a boolean is held as 1 or 0
%% and a date, time or datetime as ISO 8601 text, which kura_types:cast/2
%% already parses. A decimal held as text stays text.
load_stored(boolean, 1) ->
    true;
load_stored(boolean, 0) ->
    false;
load_stored(Type, Text) when ?IS_TEMPORAL(Type), is_binary(Text) ->
    case kura_types:cast(Type, Text) of
        {ok, Value} -> Value;
        {error, _} -> Text
    end;
load_stored(_Type, Value) ->
    Value.

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
