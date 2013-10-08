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

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Return the `Count' of error documents in the `Index'.  Error
%% documents represent objects which could not be properly extracted.
%% This typically happens when the value is a malformed version of the
%% content-type.  E.g. JSON without a closing brace.
-spec num_error_docs(index_name()) -> {ok, Count :: non_neg_integer()}
                                          | {error, Reason :: term()}.
num_error_docs(Index) ->
    try
        Params = [{q, <<"_yz_err:1">>},
                  {fl, <<"_yz_id">>},
                  {wt, <<"json">>}],
        {_Headers, Resp} = yz_solr:dist_search(Index, Params),
        Struct = mochijson2:decode(Resp),
        {ok, kvc:path([<<"response">>, <<"numFound">>], Struct)}
    catch
        _:Reason ->
            {error, {Reason, erlang:get_stacktrace()}}
    end.



