%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%%
%% @doc This module provides visbility into the state of Yokozuna.  It
%% is meant to aid in diagnosing Yokozuna when problems arise.
%%
%% All API functions should return Erlang data structures.  They
%% SHOULD NOT make logging calls or write to stdout/stderr to pass
%% information.  There may be functions which do that but they should
%% be separate.  It is very important to return Erlang data structures
%% which are properly documented and spec'd so that they can be used
%% by different consumers.
%%
%% TODO: have riak-admin diag call into this module
-module(yz_diag).
-compile(export_all).
-include("yokozuna.hrl").
-type either(Term) :: Term | {error, Reason :: term()}.
-type either_t(Term) :: Term | {error, Reason :: term(), StackTrace :: term()}.

%%%===================================================================
%%% Types of Diagnostic Information
%%%===================================================================

-type component_status() :: [{component(), boolean()}].

-type error_doc_count() :: {index_name(), either_t(non_neg_integer())}.
-type error_doc_counts() :: [error_doc_count()].

-type indexes_in_ring() :: [index_name()].

-type searcher_stat() :: {caching, boolean()} |
                         {deleted_docs, non_neg_integer()} |
                         {index_version, non_neg_integer()} |
                         {max_doc, non_neg_integer()} |
                         {num_docs, non_neg_integer()} |
                         {opened, iso8601()} |
                         {warmup_time, non_neg_integer()}.
-type searcher_stats() :: [searcher_stat()].

-type service_avail() :: [node()].

%% NOTE: If you are adding a type please keep the list in alphabetical
%% order.
%%
%% `error_doc_counts' - The number of error documents per index.
%%
%% `indexes_in_ring' - List of indexes according to the ring.
%%
%% `service_avail' - List of nodes where the Yokozuna service is available.
%%
%% `{node(), [local_datum()]}' - Datums for all nodes running the
%%   Yokozuna service.
-type diag_datum() :: {error_doc_counts, error_doc_counts()} |
                      {indexes_in_ring, indexes_in_ring()} |
                      {node(), [local_datum()]} |
                      {service_avail, service_avail()}.

%% Local datums are per-node.
%%
%% `component_status' - The status of all Yokozuna components.  `true'
%%   means enabled, `false' means disabled.
%%
%% `indexes' - List of indexes according to Solr.
%%
%% `searcher_stats' - Various Lucene Searcher statistics for each index.
-type local_datum() :: {component_status, component_status()} |
                       {indexes, [index_name()]} |
                       {searcher_stats, searcher_stats()}.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Return a list containing various diagnostic information that
%% might be useful.
-spec all() -> [diag_datum()].
all() ->
    Ring = yz_misc:get_ring(transformed),
    %% Members = riak_core_ring:all_members(Ring),
    IndexesInR = [Name || {Name, _} <- yz_index:get_indexes_from_ring(Ring)],
    ErroDocCounts = error_doc_counts(IndexesInR),
    NodesAvail = riak_core_node_watcher:nodes(?YZ_SVC_NAME),
    NodeDiags = [{N, rpc:call(N, ?MODULE, all_local, [])} || N <- NodesAvail],
    [
     {error_doc_counts, ErroDocCounts},
     {indexes_in_ring, IndexesInR},
     {service_avail, NodesAvail}|NodeDiags
    ].

%% @doc Return a list of diagnostic information for the local node.
-spec all_local() -> [local_datum()].
all_local() ->
    Indexes = indexes(),
    SearcherStats = searcher_stats(Indexes),
    [
     {component_status, component_status()},
     {indexes, Indexes},
     {searcher_stats, SearcherStats}
    ].

%% @doc Return the status of all Yokozuna components.  `true' means
%% enabled, `false' means disabled.
-spec component_status() -> component_status().
component_status() ->
    [
     {index, yokozuna:is_enabled(index)},
     {search, yokozuna:is_enabled(search)}
    ].

%% @doc Return the list of indexes, or empty list if the call to Solr
%% fails.
-spec indexes() -> Indexes :: [index_name()].
indexes() ->
    case yz_solr:cores() of
        {ok, Indexes} -> Indexes;
        _Err -> []
    end.

%% @doc Return various Lucene Searcher stats.  Either a single `Index'
%% or list of `Indexes' can be pased.
-spec searcher_stats(index_name() | [index_name()]) ->
                            either(searcher_stats() | [{node(), searcher_stats()}]).
searcher_stats(Indexes) when is_list(Indexes) ->
    [{Index, searcher_stats(Index)} || Index <- Indexes];
searcher_stats(Index) ->
    case yz_solr:mbeans_and_stats(Index) of
        {ok, JSON} ->
            MBeans = kvc:path([<<"solr-mbeans">>], mochijson2:decode(JSON)),
            CoreObj = lists:nth(2, MBeans),
            Stats = kvc:path([<<"searcher">>, <<"stats">>], CoreObj),
            Caching = kvc:path([<<"caching">>], Stats),
            DeletedDocs = kvc:path([<<"deletedDocs">>], Stats),
            IndexVersion = kvc:path([<<"indexVersion">>], Stats),
            MaxDoc = kvc:path([<<"maxDoc">>], Stats),
            NumDocs = kvc:path([<<"numDocs">>], Stats),
            Opened = kvc:path([<<"openedAt">>], Stats),
            WarmupTime = kvc:path([<<"warmupTime">>], Stats),
            [
             {caching, Caching},
             {deleted_docs, DeletedDocs},
             {index_version, IndexVersion},
             {max_doc, MaxDoc},
             {num_docs, NumDocs},
             {opened, Opened},
             {warmup_time, WarmupTime}
            ];
        Err ->
            Err
    end.

%% @doc Return the `Count' of error documents for each index in
%% `Indexes'.  Error documents represent objects which could not be
%% properly extracted.  This typically happens when the value is a
%% malformed version of the content-type.  E.g. JSON without a closing
%% brace.
-spec error_doc_counts([index_name()]) -> error_doc_counts().
error_doc_counts(Indexes) ->
    [error_doc_counts_2(Index) || Index <- Indexes].

%%%===================================================================
%%% Private
%%%===================================================================

%% @private
-spec error_doc_counts_2(index_name()) -> error_doc_count().
error_doc_counts_2(Index) ->
    try
        Params = [{q, <<"_yz_err:1">>},
                  {fl, <<"_yz_id">>},
                  {wt, <<"json">>}],
            {_Headers, Resp} = yz_solr:dist_search(Index, Params),
            Struct = mochijson2:decode(Resp),
            Count = kvc:path([<<"response">>, <<"numFound">>], Struct),
            {Index, Count}
    catch
        _:Reason ->
            {Index, {error, Reason, erlang:get_stacktrace()}}
    end.

%% @private
-spec strip_ok({ok, term()} | {error, term()}) -> either(term()).
strip_ok({ok,V}) -> V;
strip_ok(Err) -> Err.
