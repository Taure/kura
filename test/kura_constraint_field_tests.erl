-module(kura_constraint_field_tests).
-moduledoc "Which field a database constraint error is reported on.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-define(REPO, kura_constraint_field_test_repo).
-define(S, kura_test_renamed_schema).
-define(TABLE, ~"renamed_items").

constraint_field_test_() ->
    {foreach,
        fun() ->
            kura_test_recording_driver:configure(?REPO),
            kura_query_cache:flush()
        end,
        fun(_) ->
            kura_test_recording_driver:unconfigure(),
            kura_query_cache:flush()
        end,
        [
            {"a generated name maps its column to the field stored there", fun t_generated/0},
            {"the key constraint maps to the key field", fun t_key/0},
            {"a name of another kind than the error is not resolved", fun t_other_kind/0},
            {"a field name that is not a column is not resolved", fun t_field_name/0},
            {"a custom name is reported on base", fun t_custom_name/0},
            {"several columns are reported on base", fun t_several_columns/0},
            {"another table's constraint is reported on base", fun t_other_table/0},
            {"an error that names no table is reported on base", fun t_no_table/0},
            {"a composite key is reported on base", fun t_composite_key/0},
            {"a declared constraint is reported where it says", fun t_declared/0},
            {"a declaration by field covers the column's generated name", fun t_declared_column/0},
            {"a missing value maps its column to the field", fun t_not_null/0}
        ]}.

errors(Schema, Params, Error) ->
    errors(kura_changeset:cast(Schema, #{}, Params, maps:keys(Params)), Error).

errors(CS, Error) ->
    kura_test_recording_driver:reply_with([{error, {pgsql_error, Error}}]),
    {error, Failed} = kura_repo_worker:insert(?REPO, CS),
    Failed#kura_changeset.errors.

item_errors(Error) ->
    errors(?S, #{title => ~"t", qty => 1}, Error).

violation(Code, Constraint) ->
    #{code => Code, table => ?TABLE, constraint => Constraint}.

t_generated() ->
    ?assertEqual(
        [{title, ~"has already been taken"}],
        item_errors(violation(~"23505", ~"renamed_items_item_title_key"))
    ),
    ?assertEqual(
        [{title, ~"has already been taken"}],
        item_errors(violation(~"23505", ~"renamed_items_item_title_index"))
    ),
    ?assertEqual(
        [{qty, ~"does not exist"}],
        item_errors(violation(~"23503", ~"renamed_items_quantity_fkey"))
    ),
    ?assertEqual(
        [{qty, ~"is invalid"}],
        item_errors(violation(~"23514", ~"renamed_items_quantity_check"))
    ),
    ?assertEqual(
        [{note, ~"violates an exclusion constraint"}],
        item_errors(violation(~"23P01", ~"renamed_items_note_excl"))
    ).

t_key() ->
    ?assertEqual(
        [{id, ~"has already been taken"}],
        item_errors(violation(~"23505", ~"renamed_items_pkey"))
    ),
    ?assertEqual(
        [{base, ~"does not exist"}],
        item_errors(violation(~"23503", ~"renamed_items_pkey"))
    ).

t_other_kind() ->
    ?assertEqual(
        [{base, ~"does not exist"}],
        item_errors(violation(~"23503", ~"renamed_items_quantity_key"))
    ).

t_field_name() ->
    ?assertEqual(
        [{base, ~"has already been taken"}],
        item_errors(violation(~"23505", ~"renamed_items_title_key"))
    ),
    ?assertEqual(
        [{base, ~"has already been taken"}],
        item_errors(violation(~"23505", ~"renamed_items_never_stored_key"))
    ).

t_custom_name() ->
    ?assertEqual(
        [{base, ~"has already been taken"}],
        item_errors(violation(~"23505", ~"one_title_each"))
    ),
    ?assertEqual(
        [{base, ~"has already been taken"}],
        item_errors(violation(~"23505", ~"renamed_items_key"))
    ).

t_several_columns() ->
    ?assertEqual(
        [{base, ~"has already been taken"}],
        item_errors(violation(~"23505", ~"renamed_items_item_title_quantity_key"))
    ).

t_other_table() ->
    Error = #{code => ~"23503", table => ~"renamed", constraint => ~"renamed_items_note_fkey"},
    ?assertEqual([{base, ~"does not exist"}], item_errors(Error)),
    ?assertEqual(
        [{base, ~"does not exist"}],
        item_errors(#{
            code => ~"23503", table => ~"orders", constraint => ~"orders_item_id_fkey"
        })
    ).

t_no_table() ->
    ?assertEqual(
        [{base, ~"has already been taken"}],
        item_errors(#{code => ~"23505", constraint => ~"renamed_items_item_title_key"})
    ).

t_composite_key() ->
    ?assertEqual(
        [{base, ~"has already been taken"}],
        errors(
            kura_test_composite_schema,
            #{},
            #{
                code => ~"23505",
                table => kura_test_composite_schema:table(),
                constraint => <<(kura_test_composite_schema:table())/binary, "_pkey">>
            }
        )
    ).

t_declared() ->
    CS = kura_changeset:unique_constraint(
        kura_changeset:cast(?S, #{}, #{title => ~"t"}, [title]),
        qty,
        #{name => ~"one_title_each", message => ~"is taken"}
    ),
    ?assertEqual(
        [{qty, ~"is taken"}],
        errors(CS, #{code => ~"23505", constraint => ~"one_title_each"})
    ).

t_declared_column() ->
    CS = kura_changeset:unique_constraint(
        kura_changeset:cast(?S, #{}, #{title => ~"t"}, [title]),
        title,
        #{message => ~"is taken"}
    ),
    ?assertEqual(
        [{title, ~"is taken"}],
        errors(CS, violation(~"23505", ~"renamed_items_item_title_key"))
    ),
    ?assertEqual(
        [{title, ~"is taken"}],
        errors(CS, violation(~"23505", ~"renamed_items_title_key"))
    ),
    Named = kura_changeset:foreign_key_constraint(
        kura_changeset:cast(?S, #{}, #{title => ~"t"}, [title]), qty, #{name => ~"qty_ref"}
    ),
    ?assertEqual(
        [~"qty_ref"],
        [C#kura_constraint.constraint || C <- Named#kura_changeset.constraints]
    ).

t_not_null() ->
    NotNull = #{code => ~"23502", table => ?TABLE, column => ~"quantity"},
    ?assertEqual([{qty, ~"can't be blank"}], item_errors(NotNull)),
    ?assertEqual([{base, ~"can't be blank"}], item_errors(NotNull#{column := ~"qty"})),
    ?assertEqual([{base, ~"can't be blank"}], item_errors(NotNull#{column := ~"node"})),
    ?assertEqual([{base, ~"can't be blank"}], item_errors(NotNull#{table := ~"orders"})),
    ?assertEqual([{base, ~"can't be blank"}], item_errors(maps:remove(table, NotNull))).
