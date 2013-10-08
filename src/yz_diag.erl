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
-type err() :: {error, Reason :: term()}.
-type err_trace() :: {error, Reason :: term(), StackTrace :: term()}.

%%%===================================================================
%%% Types of Diagnostic Information
%%%===================================================================

%% NOTE: If you are adding a type please keep the list in alphabetical
%% order.
%%
%% `error_doc_counts' - The number of error documents per index.
%%
%% `indexes_in_ring' - List of indexes according to the ring.
%%
%% `indexes_on_node' - List of indexes according to Solr for each node.
%%
%% `service_avail' - List of nodes where the Yokozuna service is available.
-type diag_datum() :: {error_doc_counts, [{index_name(), non_neg_integer()}]} |
                      {indexes_in_ring, [index_name()]} |
                      {indexes_on_nodes, [{node(), [index_name()]}]} |
                      {service_avail, [node()]}.


%%%===================================================================
%%% API
%%%===================================================================

%% @doc Return a list containing various diagnostic information that
%% might be useful.
-spec all() -> [diag_datum()].
all() ->
    Ring = yz_misc:get_ring(transformed),
    Members = riak_core_ring:all_members(Ring),
    IndexesInR = [Name || {Name, _} <- yz_index:get_indexes_from_ring(Ring)],
    ErroDocCounts = num_error_docs(IndexesInR),
    IndexesOnNodes = indexes_on_nodes(Members),
    ServiceAvail = riak_core_node_watcher:nodes(?YZ_SVC_NAME),
    [
     {error_doc_counts, ErroDocCounts},
     {indexes_in_ring, IndexesInR},
     {indexes_on_nodes, IndexesOnNodes},
     {service_avail, ServiceAvail}
    ].

%% @doc Return the list of indexes according to Solr for each node in `Nodes'.
-spec indexes_on_nodes([node()]) -> [{node(), [index_name()] | err()}].
indexes_on_nodes(Nodes) ->
    [{Node, indexes_on_node(Node)} || Node <- Nodes].

%% @doc Return the `Count' of error documents for each index in
%% `Indexes'.  Error documents represent objects which could not be
%% properly extracted.  This typically happens when the value is a
%% malformed version of the content-type.  E.g. JSON without a closing
%% brace.
-spec num_error_docs([index_name()]) -> [{index_name(), non_neg_integer() | err_trace()}].
num_error_docs(Indexes) ->
    [num_error_docs_2(Index) || Index <- Indexes].

%%%===================================================================
%%% Private
%%%===================================================================

%% @private
-spec indexes_on_node(node()) -> {ok, [index_name()]} | {error, Reason :: term()}.
indexes_on_node(Node) ->
    strip_ok(rpc:call(Node, yz_solr, cores, [])).

%% @private
-spec num_error_docs_2(index_name()) -> {index_name(), non_neg_integer()} |
                                        {index_name(), {error, term(), term()}}.
num_error_docs_2(Index) ->
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
-spec strip_ok({ok, term()} | {error, term()}) -> term() | {error, term()}.
strip_ok({ok,V}) -> V;
strip_ok(Err) -> Err.
