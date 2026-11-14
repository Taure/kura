-module(kura_nested_cast_tests).
-moduledoc "Which of a child's fields nested params may set.".

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

-export([comment_changeset/2, label_changeset/2, log/2]).

-define(POST, kura_test_post).
-define(COMMENT, kura_test_comment).
-define(STAMP, {{2001, 2, 3}, {4, 5, 6}}).

comment_changeset(Data, Params) ->
    CS = kura_changeset:cast(?COMMENT, Data, Params, [body]),
    kura_changeset:validate_required(CS, [body]).

label_changeset(Data, Params) ->
    kura_changeset:cast(kura_test_label, Data, Params, [label]).

post(Nested) ->
    kura_changeset:cast(?POST, #{}, Nested#{title => ~"t", author_id => 1}, [title, author_id]).

profile(Nested) ->
    kura_changeset:cast(kura_test_profile, #{}, Nested#{name => ~"n"}, [name]).

comment_params() ->
    #{body => ~"b", author_id => 9, inserted_at => ?STAMP, post_id => 77, id => 5}.

child_changes(CS, Assoc) ->
    case maps:get(Assoc, CS#kura_changeset.assoc_changes) of
        Children when is_list(Children) -> [C#kura_changeset.changes || C <- Children];
        Child -> Child#kura_changeset.changes
    end.

strict(Setting, Tests) ->
    {setup,
        fun() ->
            case Setting of
                unset -> application:unset_env(kura, strict_assoc_cast);
                _ -> application:set_env(kura, strict_assoc_cast, Setting)
            end
        end,
        fun(_) -> application:unset_env(kura, strict_assoc_cast) end, Tests}.

%%----------------------------------------------------------------------
%% Allow-lists
%%----------------------------------------------------------------------

cast_list_test_() ->
    strict(true, [
        {"cast_assoc sets only the listed fields", fun() ->
            CS = kura_changeset:cast_assoc(post(#{comments => [comment_params()]}), comments, #{
                cast => [body]
            }),
            ?assert(CS#kura_changeset.valid),
            ?assertEqual([#{body => ~"b"}], child_changes(CS, comments))
        end},
        {"cast_assoc with an empty list sets nothing", fun() ->
            CS = kura_changeset:cast_assoc(post(#{comments => [comment_params()]}), comments, #{
                cast => []
            }),
            ?assertEqual([#{}], child_changes(CS, comments))
        end},
        {"many_to_many: a linked row is not edited through the link", fun() ->
            Tag = #{id => 3, name => ~"shared"},
            CS0 = kura_changeset:cast(
                ?POST, #{id => 1, tags => [Tag]}, #{tags => [#{id => 3, name => ~"renamed"}]}, []
            ),
            CS = kura_changeset:cast_assoc(CS0, tags, #{cast => []}),
            ?assertEqual([#{}], child_changes(CS, tags))
        end},
        {"put_assoc sets only the listed fields", fun() ->
            CS = kura_changeset:put_assoc(post(#{}), comments, [comment_params()], #{
                cast => [body, author_id]
            }),
            ?assertEqual([#{body => ~"b", author_id => 9}], child_changes(CS, comments))
        end},
        {"cast_embed sets only the listed fields", fun() ->
            CS = kura_changeset:cast_embed(
                profile(#{labels => [#{label => ~"l", weight => 9}]}), labels, #{cast => [label]}
            ),
            ?assertEqual([#{label => ~"l"}], kura_changeset:get_change(CS, labels))
        end}
    ]).

with_test_() ->
    strict(true, [
        {"cast_assoc with {Module, Function}", fun() ->
            CS = kura_changeset:cast_assoc(post(#{comments => [comment_params()]}), comments, #{
                with => {?MODULE, comment_changeset}
            }),
            ?assertEqual([#{body => ~"b"}], child_changes(CS, comments))
        end},
        {"the child's own validation applies", fun() ->
            CS = kura_changeset:cast_assoc(post(#{comments => [#{author_id => 1}]}), comments, #{
                with => {?MODULE, comment_changeset}
            }),
            ?assertNot(CS#kura_changeset.valid)
        end},
        {"with wins over cast", fun() ->
            CS = kura_changeset:cast_assoc(post(#{comments => [comment_params()]}), comments, #{
                with => {?MODULE, comment_changeset}, cast => [author_id]
            }),
            ?assertEqual([#{body => ~"b"}], child_changes(CS, comments))
        end},
        {"cast_assoc still takes a fun", fun() ->
            With = fun(Data, Params) ->
                kura_changeset:cast(?COMMENT, Data, Params, [author_id])
            end,
            CS = kura_changeset:cast_assoc(post(#{comments => [comment_params()]}), comments, #{
                with => With
            }),
            ?assertEqual([#{author_id => 9}], child_changes(CS, comments))
        end},
        {"put_assoc with {Module, Function}", fun() ->
            CS = kura_changeset:put_assoc(post(#{}), comments, [comment_params()], #{
                with => {?MODULE, comment_changeset}
            }),
            [Child] = maps:get(comments, CS#kura_changeset.assoc_changes),
            ?assertEqual(#{body => ~"b"}, Child#kura_changeset.changes),
            ?assertEqual(insert, Child#kura_changeset.action)
        end},
        {"cast_embed with {Module, Function}", fun() ->
            CS = kura_changeset:cast_embed(
                profile(#{labels => [#{label => ~"l", weight => 9}]}), labels, #{
                    with => {?MODULE, label_changeset}
                }
            ),
            ?assertEqual([#{label => ~"l"}], kura_changeset:get_change(CS, labels))
        end}
    ]).

invalid_options_test_() ->
    [
        ?_assertError(
            {kura, {invalid_nested_cast, ?POST, comments, {cast, Refused}}},
            kura_changeset:cast_assoc(post(#{}), comments, #{cast => Fields})
        )
     || {Fields, Refused} <- [
            {[body, id], [id]},
            {[post_id], [post_id]},
            {[body, no_such_field], [no_such_field]}
        ]
    ] ++
        [
            ?_assertError(
                {kura, {invalid_nested_cast, ?POST, comments, {cast, body}}},
                kura_changeset:cast_assoc(post(#{}), comments, #{cast => body})
            ),
            ?_assertError(
                {kura, {invalid_nested_cast, ?POST, comments, {with, ~"changeset"}}},
                kura_changeset:cast_assoc(post(#{}), comments, #{with => ~"changeset"})
            ),
            ?_assertError(
                {kura, {invalid_nested_cast, ?POST, comments, {with, _}}},
                kura_changeset:cast_assoc(post(#{}), comments, #{with => fun(P) -> P end})
            )
        ].

%%----------------------------------------------------------------------
%% strict_assoc_cast
%%----------------------------------------------------------------------

strict_refuses_a_cast_without_an_allow_list_test_() ->
    strict(true, [
        ?_assertError(
            {kura, {nested_cast_needs_allow_list, ?POST, comments}},
            kura_changeset:cast_assoc(post(#{comments => [comment_params()]}), comments)
        ),
        {"whether or not the params hold the association",
            ?_assertError(
                {kura, {nested_cast_needs_allow_list, ?POST, comments}},
                kura_changeset:cast_assoc(post(#{}), comments, #{})
            )},
        ?_assertError(
            {kura, {nested_cast_needs_allow_list, kura_test_profile, labels}},
            kura_changeset:cast_embed(profile(#{labels => [#{label => ~"l"}]}), labels)
        ),
        ?_assertError(
            {kura, {nested_cast_needs_allow_list, ?POST, comments}},
            kura_changeset:put_assoc(post(#{}), comments, [comment_params()])
        ),
        {"put_assoc of changesets needs none", fun() ->
            Child = comment_changeset(#{}, #{body => ~"b"}),
            CS = kura_changeset:put_assoc(post(#{}), comments, [Child]),
            ?assertEqual([Child], maps:get(comments, CS#kura_changeset.assoc_changes))
        end},
        {"an unknown association is still a changeset error", fun() ->
            CS = kura_changeset:cast_assoc(post(#{}), no_such_assoc),
            ?assertEqual([{no_such_assoc, ~"unknown association"}], CS#kura_changeset.errors)
        end}
    ]).

invalid_setting_test_() ->
    strict(yes, [
        ?_assertError(
            {kura, {invalid_strict_assoc_cast, yes}},
            kura_changeset:cast_assoc(post(#{}), comments)
        )
    ]).

permissive_default_test_() ->
    [
        strict(Setting, [
            {"every field but the keys is cast", fun() ->
                CS = kura_changeset:cast_assoc(post(#{comments => [comment_params()]}), comments),
                [Changes] = child_changes(CS, comments),
                ?assertEqual(#{body => ~"b", author_id => 9, inserted_at => ?STAMP}, Changes)
            end}
        ])
     || Setting <- [unset, false]
    ].

warning_test_() ->
    [
        {"unset: one warning per boot", fun() ->
            ?assertMatch(
                [#{schema := ?POST, name := comments}],
                warnings_from(unset, fun() ->
                    _ = kura_changeset:cast_assoc(
                        post(#{comments => [comment_params()]}), comments
                    ),
                    _ = kura_changeset:cast_embed(profile(#{labels => []}), labels),
                    kura_changeset:put_assoc(post(#{}), comments, [comment_params()])
                end)
            )
        end},
        {"false: no warning", fun() ->
            ?assertEqual(
                [],
                warnings_from(false, fun() ->
                    kura_changeset:cast_assoc(post(#{comments => [comment_params()]}), comments)
                end)
            )
        end},
        {"an allow-list: no warning", fun() ->
            ?assertEqual(
                [],
                warnings_from(unset, fun() ->
                    _ = kura_changeset:cast_assoc(
                        post(#{comments => [comment_params()]}), comments, #{cast => [body]}
                    ),
                    kura_changeset:put_assoc(post(#{}), comments, [
                        comment_changeset(#{}, #{body => ~"b"})
                    ])
                end)
            )
        end}
    ].

warnings_from(Setting, Run) ->
    case Setting of
        unset -> application:unset_env(kura, strict_assoc_cast);
        _ -> application:set_env(kura, strict_assoc_cast, Setting)
    end,
    kura_boot_warning:forget(strict_assoc_cast),
    ok = logger:add_handler(?MODULE, ?MODULE, #{config => self(), level => warning}),
    try
        _ = Run(),
        warnings()
    after
        logger:remove_handler(?MODULE),
        application:unset_env(kura, strict_assoc_cast),
        kura_boot_warning:forget(strict_assoc_cast)
    end.

warnings() ->
    receive
        {logged, #{name := _, schema := _} = Report} -> [Report | warnings()];
        {logged, _} -> warnings()
    after 100 -> []
    end.

log(#{msg := {report, Report}}, #{config := Pid}) ->
    Pid ! {logged, Report},
    ok;
log(_Event, _Config) ->
    ok.
