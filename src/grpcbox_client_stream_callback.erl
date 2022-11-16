-module(grpcbox_client_stream_callback).

-behaviour(grpcbox_client_stream).

-export([
    init/3,
    handle_message/2,
    handle_headers/2,
    handle_trailers/4,
    handle_eos/1
]).

-type stream_id() :: non_neg_integer().
-type callback_data() :: #{client_pid => pid(), stream_id => stream_id()}.

-spec init(pid(), stream_id(), callback_data()) -> {ok, callback_data()}.
init(_ConnectionPid, StreamId, #{client_pid := ClientPid}) ->
    {ok, #{client_pid => ClientPid, stream_id => StreamId}}.

-spec handle_message(term(), callback_data()) -> {ok, callback_data()}.
handle_message(UnmarshalledMessage, CBData) ->
    #{client_pid := ClientPid, stream_id := StreamId} = CBData,
    ClientPid ! {data, StreamId, UnmarshalledMessage},
    {ok, CBData}.

-spec handle_headers(map(), callback_data()) -> {ok, callback_data()}.
handle_headers(Metadata, CBData) ->
    #{client_pid := ClientPid, stream_id := StreamId} = CBData,
    ClientPid ! {headers, StreamId, Metadata},
    {ok, CBData}.

-spec handle_trailers(binary(), term(), map(), callback_data()) -> {ok, callback_data()}.
handle_trailers(Status, Message, Metadata, CBData) ->
    #{client_pid := ClientPid, stream_id := StreamId} = CBData,
    ClientPid ! {trailers, StreamId, {Status, Message, Metadata}},
    {ok, CBData}.

-spec handle_eos(callback_data()) -> {ok, callback_data()}.
handle_eos(CBData) ->
    #{client_pid := ClientPid, stream_id := StreamId} = CBData,
    ClientPid ! {eos, StreamId},
    {ok, CBData}.
