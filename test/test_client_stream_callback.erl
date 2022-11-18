-module(test_client_stream_callback).

-behaviour(grpcbox_client_stream).

-export([
    init/3,
    handle_message/2,
    handle_headers/2,
    handle_trailers/4,
    handle_eos/1
]).

-type stream_id() :: non_neg_integer().
-type callback_data() :: #{reply_to => pid(), ref => reference()}.

-spec init(pid(), stream_id(), term()) -> {ok, callback_data()}.
init(_ConnectionPid, _StreamId, #{reply_to := Pid, ref := Ref}) ->
    {ok, #{reply_to => Pid, ref => Ref}}.

-spec handle_message(term(), callback_data()) -> {ok, callback_data()}.
handle_message(UnmarshalledMessage, CBData) ->
    #{reply_to := Pid, ref := Ref} = CBData,
    Pid ! {data, Ref, UnmarshalledMessage},
    {ok, CBData}.

-spec handle_headers(map(), callback_data()) -> {ok, callback_data()}.
handle_headers(Metadata, CBData) ->
    #{reply_to := Pid, ref := Ref} = CBData,
    Pid ! {headers, Ref, Metadata},
    {ok, CBData}.

-spec handle_trailers(binary(), term(), map(), callback_data()) -> {ok, callback_data()}.
handle_trailers(Status, Message, Metadata, CBData) ->
    #{reply_to := Pid, ref := Ref} = CBData,
    Pid ! {trailers, Ref, {Status, Message, Metadata}},
    {ok, CBData}.

-spec handle_eos(callback_data()) -> {ok, callback_data()}.
handle_eos(CBData) ->
    #{reply_to := Pid, ref := Ref} = CBData,
    Pid ! {eos, Ref},
    {ok, CBData}.
