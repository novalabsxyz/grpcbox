-module(grpcbox_client_stream).

-export([new_stream/5,
         send_request/6,
         send_msg/2,
         recv_msg/2,

         init/3,
         on_receive_headers/2,
         on_receive_data/2,
         on_end_stream/1,
         handle_info/2]).

-include("grpcbox.hrl").

-type client_stream_callback_data() :: term().

-callback(init(ClientPid::pid(), StreamId::non_neg_integer(), term()) -> {ok, client_stream_callback_data()}).
-callback(handle_message(Message::term(), client_stream_callback_data()) -> {ok, client_stream_callback_data()}).
-callback(handle_headers(Metadata::map(), client_stream_callback_data()) -> {ok, client_stream_callback_data()}).
-callback(handle_trailers(Status::binary(), Message::term(), Metadata::map(), client_stream_callback_data()) -> {ok, client_stream_callback_data()}).
-callback(handle_eos(client_stream_callback_data()) -> {ok, client_stream_callback_data()}).

-define(headers(Scheme, Host, Path, Encoding, MessageType, MD), [{<<":method">>, <<"POST">>},
                                                                 {<<":path">>, Path},
                                                                 {<<":scheme">>, Scheme},
                                                                 {<<":authority">>, Host},
                                                                 {<<"grpc-encoding">>, Encoding},
                                                                 {<<"grpc-message-type">>, MessageType},
                                                                 {<<"content-type">>, <<"application/grpc+proto">>},
                                                                 {<<"user-agent">>, <<"grpc-erlang/0.9.2">>},
                                                                 {<<"te">>, <<"trailers">>} | MD]).

new_stream(Ctx, Channel, Path, Def=#grpcbox_def{service=Service,
                                                message_type=MessageType,
                                                marshal_fun=MarshalFun,
                                                unmarshal_fun=UnMarshalFun}, Options) ->
    case grpcbox_subchannel:conn(Channel) of
        {ok, Conn, #{scheme := Scheme,
                     authority := Authority,
                     encoding := DefaultEncoding,
                     stats_handler := StatsHandler}} ->
            Encoding = maps:get(encoding, Options, DefaultEncoding),
            Callback = callback_module(Options),
            RequestHeaders = ?headers(Scheme, Authority, Path, encoding_to_binary(Encoding),
                                      MessageType, metadata_headers(Ctx)),
            case h2_connection:new_stream(Conn, ?MODULE, [#{service => Service,
                                                            marshal_fun => MarshalFun,
                                                            unmarshal_fun => UnMarshalFun,
                                                            path => Path,
                                                            buffer => <<>>,
                                                            stats_handler => StatsHandler,
                                                            stats => #{},
                                                            callback_module => Callback,
                                                            client_pid => self()}], self()) of
                {error, _Code} = Err ->
                    Err;
                {StreamId, Pid} ->
                    h2_connection:send_headers(Conn, StreamId, RequestHeaders),
                    Ref = erlang:monitor(process, Pid),
                    {ok, #{channel => Conn,
                           stream_id => StreamId,
                           stream_pid => Pid,
                           monitor_ref => Ref,
                           service_def => Def,
                           encoding => Encoding}}
            end;
        {error, _}=Error ->
            Error
    end.

send_request(Ctx, Channel, Path, Input, #grpcbox_def{service=Service,
                                                     message_type=MessageType,
                                                     marshal_fun=MarshalFun,
                                                     unmarshal_fun=UnMarshalFun}, Options) ->
    case grpcbox_subchannel:conn(Channel) of
        {ok, Conn, #{scheme := Scheme,
                     authority := Authority,
                     encoding := DefaultEncoding,
                     stats_handler := StatsHandler}} ->
            Encoding = maps:get(encoding, Options, DefaultEncoding),
            Callback = callback_module(Options),
            Body = grpcbox_frame:encode(Encoding, MarshalFun(Input)),
            Headers = ?headers(Scheme, Authority, Path, encoding_to_binary(Encoding), MessageType, metadata_headers(Ctx)),

            %% headers are sent in the same request as creating a new stream to ensure
            %% concurrent calls can't end up interleaving the sending of headers in such
            %% a way that a lower stream id's headers are sent after another's, which results
            %% in the server closing the connection when it gets them out of order
            case h2_connection:new_stream(Conn, ?MODULE, [#{service => Service,
                                                                          marshal_fun => MarshalFun,
                                                                          unmarshal_fun => UnMarshalFun,
                                                                          path => Path,
                                                                          buffer => <<>>,
                                                                          stats_handler => StatsHandler,
                                                                          stats => #{},
                                                                          callback_module => Callback,
                                                                          client_pid => self()}], Headers, [], self()) of
                {error, _Code} = Err ->
                    Err;
                {StreamId, Pid} ->
                    h2_connection:send_body(Conn, StreamId, Body),
                    {ok, Conn, StreamId, Pid}
            end;
        {error, _}=Error ->
            Error
    end.

send_msg(#{channel := Conn,
           stream_id := StreamId,
           encoding := Encoding,
           service_def := #grpcbox_def{marshal_fun=MarshalFun}}, Input) ->
    OutFrame = grpcbox_frame:encode(Encoding, MarshalFun(Input)),
    h2_connection:send_body(Conn, StreamId, OutFrame, [{send_end_stream, false}]).

recv_msg(S=#{stream_id := Id,
             stream_pid := Pid,
             monitor_ref := Ref}, Timeout) ->
    receive
        {data, Id, V} ->
            {ok, V};
        {'DOWN', Ref, process, Pid, _Reason} ->
            case grpcbox_client:recv_trailers(S, 0) of
                {ok, {<<"0">> = _Status, _Message, _Metadata}} ->
                    stream_finished;
                {ok, {Status, Message, Metadata}} ->
                    {error, {Status, Message}, #{trailers => Metadata}};
                timeout ->
                    stream_finished
            end
    after Timeout ->
            case erlang:is_process_alive(Pid) of
                true ->
                    timeout;
                false ->
                    stream_finished
            end
    end.

metadata_headers(Ctx) ->
    case ctx:deadline(Ctx) of
        D when D =:= undefined ; D =:= infinity ->
            grpcbox_utils:encode_headers(maps:to_list(grpcbox_metadata:from_outgoing_ctx(Ctx)));
        {T, _} ->
            Timeout = {<<"grpc-timeout">>, <<(integer_to_binary(T - erlang:monotonic_time()))/binary, "S">>},
            grpcbox_utils:encode_headers([Timeout | maps:to_list(grpcbox_metadata:from_outgoing_ctx(Ctx))])
    end.

%% callbacks

init(ConnectionPid, StreamId, [_, State=#{path := Path, callback_module := {CallbackModule, CallbackInitArgs}}]) ->
    _ = process_flag(trap_exit, true),
    Ctx1 = ctx:with_value(ctx:new(), grpc_client_method, Path),
    State1 = stats_handler(Ctx1, rpc_begin, {}, State),
    {ok, CallbackData} = init_stream_callback(CallbackModule, CallbackInitArgs, ConnectionPid, StreamId), 
    {ok, State1#{stream_id => StreamId, callback_data => CallbackData}};
init(_, _, State) ->
    {ok, State}.

on_receive_headers(H, State=#{resp_headers := _,
                              ctx := Ctx} = State) ->
    Status = proplists:get_value(<<"grpc-status">>, H, undefined),
    Message = proplists:get_value(<<"grpc-message">>, H, undefined),
    Metadata = grpcbox_utils:headers_to_metadata(H),
    State1 = handle_trailers_stream_callback(Status, Message, Metadata, State),
    Ctx1 = ctx:with_value(Ctx, grpc_client_status, grpcbox_utils:status_to_string(Status)),
    {ok, State1#{ctx := Ctx1,
                resp_trailers => H}};
on_receive_headers(H, State=#{ctx := Ctx} = State) ->
    Encoding = proplists:get_value(<<"grpc-encoding">>, H, identity),
    Metadata = grpcbox_utils:headers_to_metadata(H),

    State1 = handle_headers_stream_callback(Metadata, State),
    %% TODO: better way to know if it is a Trailers-Only response?
    %% maybe chatterbox should include information about the end of the stream
    case proplists:get_value(<<"grpc-status">>, H, undefined) of
        undefined ->
            {ok, State1#{resp_headers => H,
                        encoding => encoding_to_atom(Encoding)}};
        Status ->
            Message = proplists:get_value(<<"grpc-message">>, H, undefined),
            State2 = handle_trailers_stream_callback(Status, Message, Metadata, State),
            Ctx1 = ctx:with_value(Ctx, grpc_client_status, grpcbox_utils:status_to_string(Status)),
            {ok, State2#{resp_headers => H,
                        ctx => Ctx1,
                        status => Status,
                        encoding => encoding_to_atom(Encoding)}}
    end.

on_receive_data(Data, State=#{buffer := Buffer,
                              encoding := Encoding} = State) ->
    {Remaining, Messages} = grpcbox_frame:split(<<Buffer/binary, Data/binary>>, Encoding),
    State1 = handle_message_stream_callback(Messages, State),
    {ok, State1#{buffer => Remaining}};
on_receive_data(_Data, State) ->
    {ok, State}.

on_end_stream(State=#{ctx := Ctx}) ->
    State1 = handle_eos_stream_callback(State),
    State2 = stats_handler(Ctx, rpc_end, {}, State1),
    {ok, State2}.

handle_info(_, State) ->
    State.

%%

callback_module(#{callback_module := CallbackModule}) ->
    CallbackModule;
callback_module(_) ->
    {grpcbox_client_stream_callback, #{client_pid => self()}}.

init_stream_callback(CallbackModule, CallbackInitArgs, ConnectionPid, StreamId) ->
    CallbackModule:init(ConnectionPid, StreamId, CallbackInitArgs).

handle_message_stream_callback(Messages, State) ->
    #{unmarshal_fun := UnmarshalFun,
      callback_module := {CallbackModule, _},
      callback_data := CallbackData} = State,
    CallbackData1 = lists:foldl(
                        fun(M, D) ->
                            {ok, D1} = CallbackModule:handle_message(UnmarshalFun(M), D),
                            D1
                        end, CallbackData, Messages),
    State#{callback_data := CallbackData1}.

handle_trailers_stream_callback(Status, Message, Metadata, State) ->
    #{callback_module := {CallbackModule, _},
      callback_data := CallbackData} = State,
    {ok, NewCallbackData} = CallbackModule:handle_trailers(Status, Message, Metadata, CallbackData),
    State#{callback_data := NewCallbackData}.

handle_headers_stream_callback(Metadata, State) ->
    #{callback_module := {CallbackModule, _},
      callback_data := CallbackData} = State,
    {ok, NewCallbackData} = CallbackModule:handle_headers(Metadata, CallbackData),
    State#{callback_data := NewCallbackData}.

handle_eos_stream_callback(State) ->
    #{callback_module := {CallbackModule, _},
      callback_data := CallbackData} = State,
    {ok, NewCallbackData} = CallbackModule:handle_eos(CallbackData),
    State#{callback_data := NewCallbackData}.


stats_handler(Ctx, _, _, State=#{stats_handler := undefined}) ->
    State#{ctx => Ctx};
stats_handler(Ctx, Event, Stats, State=#{stats_handler := StatsHandler,
                                         stats := StatsState}) ->
    {Ctx1, StatsState1} = StatsHandler:handle(Ctx, client, Event, Stats, StatsState),
    State#{ctx => Ctx1,
           stats => StatsState1}.

encoding_to_atom(identity) -> identity;
encoding_to_atom(<<"identity">>) -> identity;
encoding_to_atom(<<"gzip">>) -> gzip;
encoding_to_atom(<<"deflate">>) -> deflate;
encoding_to_atom(<<"snappy">>) -> snappy;
encoding_to_atom(Custom) -> binary_to_atom(Custom, latin1).

encoding_to_binary(identity) -> <<"identity">>;
encoding_to_binary(gzip) -> <<"gzip">>;
encoding_to_binary(deflate) -> <<"deflate">>;
encoding_to_binary(snappy) -> <<"snappy">>;
encoding_to_binary(Custom) -> atom_to_binary(Custom, latin1).

