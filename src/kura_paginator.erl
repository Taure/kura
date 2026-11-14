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

## Maximum page size

`page_size` and `limit` usually come from a request, so cap them:

```erlang
{kura, [{max_page_size, 200}]}
```

or per call with the `max_page_size` option, which wins over the
application setting. A larger size returns
`{error, {page_size_too_large, Size, Max}}`; it is refused, not silently
lowered. `infinity` turns the cap off on purpose. Any other value, as the
setting or as the option, raises
`{kura, {invalid_setting, max_page_size, Value}}`.

With no `max_page_size` set, nothing is capped, and the first request for
more than 1000 rows logs one warning per boot. In kura 3.0 the default
becomes 1000.
""".

-include("kura.hrl").

-export([paginate/3, cursor_paginate/3, total_pages/2]).

-define(DEFAULT_PAGE_SIZE, 20).
-define(NEXT_MAJOR_MAX_PAGE_SIZE, 1000).

-doc """
Offset-based pagination. Returns entries for the given page along with
total counts.

Options:
- `page` - page number, 1-based (default: 1)
- `page_size` - entries per page (default: 20)
- `max_page_size` - the largest `page_size` accepted, or `infinity`
  (default: the `max_page_size` application setting)

`page` and `page_size` must be positive integers; anything else returns
`{error, {invalid_page, Page}}` or `{error, {invalid_page_size, Size}}`
without touching the database. A `page_size` over the maximum returns
`{error, {page_size_too_large, Size, Max}}`. See the moduledoc for the
maximum.
""".
-spec paginate(module(), #kura_query{}, map()) -> {ok, map()} | {error, term()}.
paginate(Repo, Query, Opts) ->
    Page = maps:get(page, Opts, 1),
    case page_size(page_size, maps:get(page_size, Opts, ?DEFAULT_PAGE_SIZE), Opts) of
        {ok, PageSize} when is_integer(Page), Page >= 1 ->
            paginate(Repo, Query, Page, PageSize);
        {ok, _PageSize} ->
            {error, {invalid_page, Page}};
        {error, _} = Err ->
            Err
    end.

paginate(Repo, Query, Page, PageSize) ->
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
                    TotalPages = total_pages(TotalEntries, PageSize),
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
Cursor-based (keyset) pagination. Efficient for large datasets - avoids
counting and offset scanning.

Options:
- `limit` - max entries to return (default: 20)
- `max_page_size` - the largest `limit` accepted, or `infinity` (default:
  the `max_page_size` application setting)
- `cursor_field` - field to paginate on (default: `id`)
- `'after'` - cursor value; fetch entries after this value (forward)
- `before` - cursor value; fetch entries before this value (backward)

When neither `after` nor `before` is given, fetches from the beginning.

`limit` must be a positive integer, or the call returns
`{error, {invalid_limit, Limit}}`. A `limit` over the maximum returns
`{error, {page_size_too_large, Limit, Max}}`.

The query's own `order_by` is replaced by the cursor field. A vector
distance in it raises `{kura, {distance_not_supported_here,
cursor_paginate}}` rather than being dropped; ordering by the alias of a
selected distance is dropped like any other ordering.
""".
-spec cursor_paginate(module(), #kura_query{}, map()) -> {ok, map()} | {error, term()}.
cursor_paginate(Repo, Query, Opts) ->
    reject_distance_order(Query),
    case page_size(limit, maps:get(limit, Opts, ?DEFAULT_PAGE_SIZE), Opts) of
        {ok, Limit} -> cursor_paginate(Repo, Query, Opts, Limit);
        {error, _} = Err -> Err
    end.

cursor_paginate(Repo, Query, Opts, Limit) ->
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
                    true -> take_rows(Rows, Limit);
                    false -> Rows
                end,
            Entries =
                case Direction of
                    forward -> Trimmed;
                    backward -> reverse_rows(Trimmed, [])
                end,
            {StartCursor, EndCursor} = extract_cursors(Entries, CursorField),
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

reject_distance_order(#kura_query{order_bys = Orders}) ->
    case [D || {{distance, _, _, _} = D, _Dir} <- Orders] of
        [] -> ok;
        _ -> erlang:error({kura, {distance_not_supported_here, cursor_paginate}})
    end.

-spec page_size(page_size | limit, term(), map()) ->
    {ok, pos_integer()}
    | {error, {invalid_page_size | invalid_limit, term()}}
    | {error, {page_size_too_large, pos_integer(), pos_integer()}}.
page_size(_Option, Size, Opts) when is_integer(Size), Size >= 1 ->
    case max_page_size(Opts) of
        infinity ->
            {ok, Size};
        Max when is_integer(Max), Size > Max ->
            {error, {page_size_too_large, Size, Max}};
        Max when is_integer(Max) ->
            {ok, Size};
        unset when Size > ?NEXT_MAJOR_MAX_PAGE_SIZE ->
            kura_boot_warning:once(max_page_size, #{
                msg => ~"kura: page size is not capped because max_page_size is not set",
                requested => Size,
                next_major_default => ?NEXT_MAJOR_MAX_PAGE_SIZE,
                fix => ~"set {kura, [{max_page_size, N}]} or pass max_page_size"
            }),
            {ok, Size};
        unset ->
            {ok, Size}
    end;
page_size(page_size, Size, _Opts) ->
    {error, {invalid_page_size, Size}};
page_size(limit, Size, _Opts) ->
    {error, {invalid_limit, Size}}.

-spec max_page_size(map()) -> pos_integer() | infinity | unset.
max_page_size(#{max_page_size := Max}) ->
    kura_settings:checked_max_page_size(Max);
max_page_size(_Opts) ->
    kura_settings:max_page_size().

-doc "Calculate total number of pages given total entries and page size.".
-spec total_pages(non_neg_integer(), pos_integer()) -> non_neg_integer().
total_pages(0, _PageSize) -> 0;
total_pages(TotalEntries, PageSize) -> (TotalEntries + PageSize - 1) div PageSize.

-spec extract_cursors([map()], atom()) -> {term(), term()}.
extract_cursors([], _CursorField) ->
    {undefined, undefined};
extract_cursors([First | _] = Entries, CursorField) ->
    Last = last_row(Entries),
    {maps:get(CursorField, First), maps:get(CursorField, Last)}.

-spec last_row([map()]) -> map().
last_row([H]) -> H;
last_row([_ | T]) -> last_row(T).

-spec take_rows([map()], non_neg_integer()) -> [map()].
take_rows(_, 0) -> [];
take_rows([], _) -> [];
take_rows([H | T], N) -> [H | take_rows(T, N - 1)].

-spec reverse_rows([map()], [map()]) -> [map()].
reverse_rows([], Acc) -> Acc;
reverse_rows([H | T], Acc) -> reverse_rows(T, [H | Acc]).
