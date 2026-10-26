-module(kura_db_raising_init_repo).
-behaviour(kura_repo).

-export([otp_app/0, init/1]).

otp_app() -> kura.

init(#{raise := function_clause} = Config) ->
    unmatched(Config);
init(#{raise := invalid_config} = Config) ->
    error({invalid_config, Config});
init(#{raise := untagged}) ->
    error({~"s3cret", oops});
init(#{raise := empty}) ->
    exit({});
init(#{raise := thrown}) ->
    throw([~"s3cret"]);
init(#{raise := fetched_secret}) ->
    ok = vault_password();
init(#{password := Password} = Config) ->
    error({bad_config, Password, Config}).

unmatched(#{never := _}) ->
    #{}.

vault_password() ->
    persistent_term:get({?MODULE, vault_password}, ~"supersecretvaultpw").
