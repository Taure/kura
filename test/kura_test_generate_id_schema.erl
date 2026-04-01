-module(kura_test_generate_id_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0, generate_id/0]).

table() -> ~"generate_id_items".

fields() ->
    [
        #kura_field{name = id, type = uuid, primary_key = true, nullable = false},
        #kura_field{name = name, type = string, nullable = false},
        #kura_field{name = inserted_at, type = utc_datetime},
        #kura_field{name = updated_at, type = utc_datetime}
    ].

generate_id() ->
    <<A:48, _:4, B:12, _:2, C:62>> = crypto:strong_rand_bytes(16),
    Bytes = <<A:48, 7:4, B:12, 2:2, C:62>>,
    Hex = binary:encode_hex(Bytes, lowercase),
    <<P1:8/binary, P2:4/binary, P3:4/binary, P4:4/binary, P5:12/binary>> = Hex,
    <<P1/binary, "-", P2/binary, "-", P3/binary, "-", P4/binary, "-", P5/binary>>.
