-module(kura_schema_verify_test_method_schema).
-behaviour(kura_schema).

-include_lib("kura/include/kura.hrl").

-export([table/0, fields/0, indexes/0]).

table() -> ~"kura_verify_test_method".

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = email, type = string},
        #kura_field{name = name, type = string}
    ].

indexes() ->
    [{[email], #{using => hash, name => ~"by_email_hash"}}, {[name], #{}}].
