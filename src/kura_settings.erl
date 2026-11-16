-module(kura_settings).
-moduledoc false.

-include_lib("kernel/include/logger.hrl").

-export([flag/1, checked_flag/2, max_page_size/0, checked_max_page_size/1]).
-export([read/1, validate/0, validate_repo/1]).

-export_type([key/0, flag/0]).

-type flag() :: require_tenant | strict_assoc_cast | strict_locks | telemetry_error_detail.
-type key() :: flag() | max_page_size | encryption_write_format.

-define(KEYS, [
    encryption_write_format,
    require_tenant,
    strict_assoc_cast,
    strict_locks,
    telemetry_error_detail,
    max_page_size
]).

-spec flag(flag()) -> boolean() | unset.
flag(Key) ->
    case application:get_env(kura, Key) of
        undefined -> unset;
        {ok, Value} -> checked_flag(Key, Value)
    end.

-spec checked_flag(flag(), term()) -> boolean().
checked_flag(_Key, Value) when is_boolean(Value) -> Value;
checked_flag(Key, Value) -> invalid(Key, Value).

-spec max_page_size() -> pos_integer() | infinity | unset.
max_page_size() ->
    case application:get_env(kura, max_page_size) of
        undefined -> unset;
        {ok, Value} -> checked_max_page_size(Value)
    end.

-spec checked_max_page_size(term()) -> pos_integer() | infinity.
checked_max_page_size(infinity) -> infinity;
checked_max_page_size(Max) when is_integer(Max), Max >= 1 -> Max;
checked_max_page_size(Value) -> invalid(max_page_size, Value).

-spec read(key()) -> {ok, term()} | unset | {invalid, term()}.
read(Key) ->
    case application:get_env(kura, Key) of
        undefined ->
            unset;
        {ok, Value} ->
            case valid(Key, Value) of
                true -> {ok, Value};
                false -> {invalid, Value}
            end
    end.

-spec validate() -> ok | {error, {invalid_setting, key(), term()}}.
validate() ->
    validate(?KEYS).

-spec validate([key()]) -> ok | {error, {invalid_setting, key(), term()}}.
validate([]) ->
    ok;
validate([Key | Rest]) ->
    case read(Key) of
        {invalid, Value} ->
            ?LOG_ERROR(#{
                msg => ~"kura: a setting in the kura application environment is not valid",
                setting => Key,
                value => Value,
                expected => expected(Key)
            }),
            {error, {invalid_setting, Key, Value}};
        _ValidOrUnset ->
            validate(Rest)
    end.

%% A repo's own require_tenant, and the application environment's that
%% stands for it when the repo sets none.
-spec validate_repo(map()) -> ok | {error, {invalid_setting, key(), term()}}.
validate_repo(#{require_tenant := Required}) when is_boolean(Required) ->
    ok;
validate_repo(#{require_tenant := Value}) ->
    {error, {invalid_setting, require_tenant, Value}};
validate_repo(_Config) ->
    case read(require_tenant) of
        {invalid, Value} -> {error, {invalid_setting, require_tenant, Value}};
        _ValidOrUnset -> ok
    end.

-spec valid(key(), term()) -> boolean().
valid(max_page_size, infinity) -> true;
valid(max_page_size, Max) -> is_integer(Max) andalso Max >= 1;
valid(encryption_write_format, Format) -> Format =:= v1 orelse Format =:= v2;
valid(_Flag, Value) -> is_boolean(Value).

-spec expected(key()) -> binary().
expected(max_page_size) -> ~"a positive integer or infinity";
expected(encryption_write_format) -> ~"v1 or v2";
expected(_Flag) -> ~"true or false".

-spec invalid(key(), term()) -> no_return().
invalid(Key, Value) ->
    erlang:error({kura, {invalid_setting, Key, Value}}).
