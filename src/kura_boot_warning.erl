-module(kura_boot_warning).
-moduledoc """
Logs a warning the first time it is asked to and stays quiet after that,
until the node restarts. Kura uses it where a default is kept for
compatibility although a safer setting exists.
""".

-include_lib("kernel/include/logger.hrl").

-export([once/2, forget/1]).

-doc "Log `Report` as a warning unless a warning for `Key` was logged since boot.".
-spec once(atom(), logger:report()) -> ok.
once(Key, Report) ->
    case persistent_term:get({?MODULE, Key}, pending) of
        logged ->
            ok;
        pending ->
            persistent_term:put({?MODULE, Key}, logged),
            ?LOG_WARNING(Report)
    end.

-doc "Let the warning for `Key` be logged again.".
-spec forget(atom()) -> ok.
forget(Key) ->
    _ = persistent_term:erase({?MODULE, Key}),
    ok.
