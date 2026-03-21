# Pagination

Kura provides two pagination strategies through `kura_paginator`: offset-based and cursor-based. Both work on top of standard `kura_query` queries.

## Offset-Based Pagination

Offset-based pagination fetches a numbered page of results along with total counts. This is the simplest approach and works well for small-to-medium datasets where you need page numbers in the UI.

```erlang
Q = kura_query:from(my_user),
{ok, Page} = kura_paginator:paginate(my_repo, Q, #{page => 2, page_size => 25}).
```

The returned map contains:

```erlang
#{
    entries => [#{id => 26, name => ~"Alice"}, ...],
    page => 2,
    page_size => 25,
    total_entries => 150,
    total_pages => 6
}
```

Both `page` and `page_size` are optional. They default to `1` and `20` respectively:

```erlang
%% First page, 20 entries per page
{ok, Page} = kura_paginator:paginate(my_repo, Q, #{}).
```

You can combine pagination with any query filters or ordering:

```erlang
Q = kura_query:from(my_user),
Q1 = kura_query:where(Q, {active, true}),
Q2 = kura_query:order_by(Q1, {name, asc}),
{ok, Page} = kura_paginator:paginate(my_repo, Q2, #{page => 1, page_size => 10}).
```

### Total Pages Helper

`total_pages/2` computes the number of pages for a given total and page size:

```erlang
8 = kura_paginator:total_pages(150, 20).
0 = kura_paginator:total_pages(0, 20).
1 = kura_paginator:total_pages(1, 20).
```

## Cursor-Based Pagination

Cursor-based (keyset) pagination navigates through results using a cursor value rather than page numbers. It avoids `COUNT(*)` and `OFFSET` scanning, making it significantly faster for large datasets.

```erlang
Q = kura_query:from(my_user),
{ok, Page} = kura_paginator:cursor_paginate(my_repo, Q, #{limit => 20}).
```

The returned map contains:

```erlang
#{
    entries => [#{id => 1, name => ~"Alice"}, ...],
    has_next => true,
    has_prev => false,
    start_cursor => 1,
    end_cursor => 20
}
```

### Navigating Forward

Use the `end_cursor` from the previous page as the `after` value:

```erlang
{ok, Page1} = kura_paginator:cursor_paginate(my_repo, Q, #{limit => 20}),
EndCursor = maps:get(end_cursor, Page1),
{ok, Page2} = kura_paginator:cursor_paginate(my_repo, Q, #{
    limit => 20,
    'after' => EndCursor
}).
```

### Navigating Backward

Use the `start_cursor` from the current page as the `before` value:

```erlang
StartCursor = maps:get(start_cursor, Page2),
{ok, PrevPage} = kura_paginator:cursor_paginate(my_repo, Q, #{
    limit => 20,
    before => StartCursor
}).
```

### Custom Cursor Field

By default, cursor pagination uses the `id` field. You can paginate on any field:

```erlang
{ok, Page} = kura_paginator:cursor_paginate(my_repo, Q, #{
    limit => 20,
    cursor_field => created_at
}).
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `limit` | `20` | Maximum entries to return |
| `cursor_field` | `id` | Field to paginate on |
| `after` | `undefined` | Fetch entries after this cursor value (forward) |
| `before` | `undefined` | Fetch entries before this cursor value (backward) |

Passing both `after` and `before` simultaneously raises `badarg`.

## When to Use Which

**Offset-based** is a good fit when:

- You need numbered pages (e.g., "Page 3 of 12")
- The dataset is small to moderate
- Users need to jump to arbitrary pages

**Cursor-based** is a good fit when:

- The dataset is large (offset scanning gets slower as page numbers increase)
- You only need next/previous navigation (infinite scroll, "Load More")
- Consistent performance matters regardless of how deep into the results you are
- Results may change between requests (offsets can skip or duplicate rows when data is inserted/deleted)
