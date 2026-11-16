-module(kura_boot_warning).
-moduledoc false.

-include_lib("kernel/include/logger.hrl").

-export([once/2, forget/1]).

-spec once(term(), logger:report()) -> ok.
once(Key, Report) ->
    case persistent_term:get({?MODULE, Key}, pending) of
        logged ->
            ok;
        pending ->
            persistent_term:put({?MODULE, Key}, logged),
            ?LOG_WARNING(Report)
    end.

-doc "Let the warning for `Key` be logged again.".
-spec forget(term()) -> ok.
forget(Key) ->
    _ = persistent_term:erase({?MODULE, Key}),
    ok.
