-module(kura_audit_encryption_tests).
-include_lib("eunit/include/eunit.hrl").

-define(SCHEMA, kura_test_encrypted_schema).

insert_redacts_encrypted_field_test() ->
    Record = #{id => 1, name => <<"Alice">>, ssn => <<"123-45-6789">>},
    {undefined, New, undefined} = kura_audit:build_audit_data(?SCHEMA, insert, Record),
    ?assertEqual(<<"[encrypted]">>, maps:get(ssn, New)),
    ?assertEqual(<<"Alice">>, maps:get(name, New)).

delete_redacts_encrypted_field_test() ->
    Record = #{id => 1, name => <<"Bob">>, ssn => <<"999-99-9999">>},
    {Old, undefined, undefined} = kura_audit:build_audit_data(?SCHEMA, delete, Record),
    ?assertEqual(<<"[encrypted]">>, maps:get(ssn, Old)),
    ?assertEqual(<<"Bob">>, maps:get(name, Old)).

update_redacts_but_keeps_change_signal_test() ->
    Old = #{id => 1, name => <<"A">>, ssn => <<"old-ssn">>},
    New = #{id => 1, name => <<"B">>, ssn => <<"new-ssn">>},
    erlang:put({kura_audit_stash, ?SCHEMA}, Old),
    {OldR, NewR, Changes} = kura_audit:build_audit_data(?SCHEMA, update, New),
    ?assertEqual(<<"[encrypted]">>, maps:get(ssn, OldR)),
    ?assertEqual(<<"[encrypted]">>, maps:get(ssn, NewR)),
    %% ssn changed -> the entry survives (signal) but both sides are redacted
    ?assertEqual(#{old => <<"[encrypted]">>, new => <<"[encrypted]">>}, maps:get(ssn, Changes)),
    %% non-encrypted change keeps its plaintext diff
    ?assertEqual(#{old => <<"A">>, new => <<"B">>}, maps:get(name, Changes)).

update_unchanged_encrypted_absent_from_diff_test() ->
    %% diff runs on plaintext BEFORE redaction, so an unchanged encrypted
    %% field is correctly absent from changes (not a false sentinel==sentinel).
    Old = #{id => 1, name => <<"A">>, ssn => <<"same">>},
    New = #{id => 1, name => <<"B">>, ssn => <<"same">>},
    erlang:put({kura_audit_stash, ?SCHEMA}, Old),
    {_OldR, _NewR, Changes} = kura_audit:build_audit_data(?SCHEMA, update, New),
    ?assertNot(maps:is_key(ssn, Changes)),
    ?assert(maps:is_key(name, Changes)).
