-module(kura_paginator).
-moduledoc """
Pagination helpers for Kura queries.

Offset-based pagination:

```erlang
Q = kura_query:from(my_schema),
{ok, Page} = kura_paginator:paginate(my_repo, Q, #{page => 2, page_size => 20}).
%% #{entries => [...], page => 2, page_size => 20,
%%   total_entries => 150, total_pages => 8}
```

Cursor-based (keyset) pagination:

```erlang
Q = kura_query:from(my_schema),
{ok, Page} = kura_paginator:cursor_paginate(my_repo, Q, #{limit => 20}).
%% #{entries => [...], has_next => true, has_prev => false,
%%   start_cursor => 1, end_cursor => 20}

%% Next page:
{ok, Page2} = kura_paginator:cursor_paginate(my_repo, Q, #{
    limit => 20, after => maps:get(end_cursor, Page)
}).
```
""".

-include("kura.hrl").

-export([paginate/3, cursor_paginate/3]).

-doc """
Offset-based pagination. Returns entries for the given page along with
total counts.

Options:
- `page` — page number, 1-based (default: 1)
- `page_size` — entries per page (default: 20)
""".
-spec paginate(module(), #kura_query{}, map()) -> {ok, map()} | {error, term()}.
paginate(Repo, Query, Opts) ->
    Page = maps:get(page, Opts, 1),
    PageSize = maps:get(page_size, Opts, 20),

    CountQ = Query#kura_query{
        select = [{count, '*'}],
        order_bys = [],
        limit = undefined,
        offset = undefined,
        preloads = [],
        distinct = false
    },
    case kura_repo_worker:all(Repo, CountQ) of
        {ok, [#{count := TotalEntries}]} ->
            Offset = (Page - 1) * PageSize,
            DataQ = Query#kura_query{
                limit = PageSize,
                offset = Offset
            },
            case kura_repo_worker:all(Repo, DataQ) of
                {ok, Entries} ->
                    TotalPages =
                        case TotalEntries of
                            0 -> 0;
                            N -> (N + PageSize - 1) div PageSize
                        end,
                    {ok, #{
                        entries => Entries,
                        page => Page,
                        page_size => PageSize,
                        total_entries => TotalEntries,
                        total_pages => TotalPages
                    }};
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

-doc """
Cursor-based (keyset) pagination. Efficient for large datasets — avoids
counting and offset scanning.

Options:
- `limit` — max entries to return (default: 20)
- `cursor_field` — field to paginate on (default: `id`)
- `'after'` — cursor value; fetch entries after this value (forward)
- `before` — cursor value; fetch entries before this value (backward)

When neither `after` nor `before` is given, fetches from the beginning.
""".
-spec cursor_paginate(module(), #kura_query{}, map()) -> {ok, map()} | {error, term()}.
cursor_paginate(Repo, Query, Opts) ->
    Limit = maps:get(limit, Opts, 20),
    CursorField = maps:get(cursor_field, Opts, id),

    {Direction, CursorValue} =
        case {maps:get('after', Opts, undefined), maps:get(before, Opts, undefined)} of
            {undefined, undefined} -> {forward, undefined};
            {V, undefined} -> {forward, V};
            {undefined, V} -> {backward, V};
            {_, _} -> error(badarg)
        end,

    Q1 =
        case CursorValue of
            undefined ->
                Query;
            _ when Direction =:= forward ->
                kura_query:where(Query, {CursorField, '>', CursorValue});
            _ ->
                kura_query:where(Query, {CursorField, '<', CursorValue})
        end,

    OrderDir =
        case Direction of
            forward -> asc;
            backward -> desc
        end,
    Q2 = Q1#kura_query{
        order_bys = [{CursorField, OrderDir}],
        limit = Limit + 1
    },

    case kura_repo_worker:all(Repo, Q2) of
        {ok, Rows} ->
            HasMore = length(Rows) > Limit,
            Trimmed =
                case HasMore of
                    true -> lists:sublist(Rows, Limit);
                    false -> Rows
                end,
            Entries =
                case Direction of
                    forward -> Trimmed;
                    backward -> lists:reverse(Trimmed)
                end,
            {StartCursor, EndCursor} =
                case Entries of
                    [] ->
                        {undefined, undefined};
                    _ ->
                        {
                            maps:get(CursorField, hd(Entries)),
                            maps:get(CursorField, lists:last(Entries))
                        }
                end,
            HasNext =
                case Direction of
                    forward -> HasMore;
                    backward -> CursorValue =/= undefined
                end,
            HasPrev =
                case Direction of
                    forward -> CursorValue =/= undefined;
                    backward -> HasMore
                end,
            {ok, #{
                entries => Entries,
                has_next => HasNext,
                has_prev => HasPrev,
                start_cursor => StartCursor,
                end_cursor => EndCursor
            }};
        {error, _} = Err ->
            Err
    end.
