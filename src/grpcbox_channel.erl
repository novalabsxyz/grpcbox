-module(grpcbox_channel).

-behaviour(gen_statem).

-export([start_link/3,
         is_ready/1,
         pick/2,
         stop/1]).
-export([init/1,
         callback_mode/0,
         terminate/3,
         connected/3,
         idle/3]).

-include("grpcbox.hrl").

-define(CHANNEL(Name), {via, gproc, {n, l, {?MODULE, Name}}}).

-type t() :: any().
-type name() :: t().
-type transport() :: http | https.
-type host() :: inet:ip_address() | inet:hostname().
-type endpoint() :: {transport(), host(), inet:port_number(), ssl:ssl_option()} | %% old style, no connection count
                    {transport(), host(), inet:port_number(), ssl:ssl_option(), pos_integer()}. %% with connection count

-type options() :: #{balancer => load_balancer(),
                     encoding => gprcbox:encoding(),
                     unary_interceptor => grpcbox_client:unary_interceptor(),
                     stream_interceptor => grpcbox_client:stream_interceptor(),
                     stats_handler => module(),
                     sync_start => boolean()}.
-type load_balancer() :: round_robin | random | hash | direct | claim.
-export_type([t/0,
              name/0,
              options/0,
              endpoint/0]).

-record(data, {endpoints :: [endpoint()],
               pool :: atom(),
               resolver :: module(),
               balancer :: grpcbox:balancer(),
               encoding :: grpcbox:encoding(),
               interceptors :: #{unary_interceptor => grpcbox_client:unary_interceptor(),
                                 stream_interceptor => grpcbox_client:stream_interceptor()}
                             | undefined,
               stats_handler :: module() | undefined,
               refresh_interval :: timer:time()}).

-spec start_link(name(), [endpoint()], options()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Name, Endpoints, Options) ->
    gen_statem:start_link(?CHANNEL(Name), ?MODULE, [Name, Endpoints, Options], []).

-spec is_ready(name()) -> boolean().
is_ready(Name) ->
    gen_statem:call(?CHANNEL(Name), is_ready).

%% @doc Picks a subchannel from a pool using the configured strategy.
-spec pick(name(), unary | stream) -> {ok, {pid(), grpcbox_client:interceptor() | undefined}} |
                                   {error, undefined_channel | no_endpoints}.

pick(Name, CallType) ->
    pick(Name, CallType, []).

pick(Name, CallType, Acc) ->
    try
        case gproc_pool:pick(Name) of
            false -> {error, no_endpoints};
            GProcName ->
                Pid = gproc:where(GProcName),
                case application:get_env(grpcbox, max_client_streams, undefined) of
                    undefined ->
                        {ok, {Pid, interceptor(Name, CallType)}};
                    MaxCount when is_integer(MaxCount) ->
                        %% TODO is there a better way to go from a global gproc
                        %% name to a local one?
                        {n,l,[gproc_pool,_,_,LocalName]} = GProcName,
                        case lists:member(LocalName, Acc) andalso length(lists:usort(Acc)) == length(gproc_pool:active_workers(Name)) of
                            true ->
                                %% we've checked all the workers, and they're all full
                                %% time to start another one
                                Workers = gproc_pool:active_workers(Name),
                                NewCount = lists:max([C || {{_Transport, _Host, _Port, _SSLOpts, C}, _Pid} <- Workers]) + 1,
                                %% TODO if there's multiple hosts defined, maybe pick the 
                                %% one with the lowest count?
                                {Transport, Host, Port, SSLOptions, _} = LocalName,
                                NewName = {Transport, Host, Port, SSLOptions, NewCount},
                                {ok, _Conn, #{encoding := Encoding, stats_handler := StatsHandler}} = grpcbox_subchannel:conn(Pid),
                                try gproc_pool:add_worker(Name, NewName) of
                                    _ ->
                                        {ok, NewPid} = grpcbox_subchannel:start_link(NewName, Name,
                                                                                     {Transport, Host, Port, SSLOptions},
                                                                                     Encoding, StatsHandler),
                                        {ok, {NewPid, interceptor(Name, CallType)}}
                                catch
                                    error:exists ->
                                        %% race to create it
                                        NewPid = gproc:where(GProcName),
                                        {ok, {NewPid, interceptor(Name, CallType)}}
                                end;
                            false ->
                                {ok, Conn, _Info} = grpcbox_subchannel:conn(Pid),
                                ActiveCount = h2_stream_set:my_active_count(Conn),
                                case ActiveCount >= MaxCount of
                                    true ->
                                        %% this stream is overloaded
                                        %% try to find another one
                                        pick(Name, CallType, lists:usort([LocalName|Acc]));
                                    false ->
                                        {ok, {Pid, interceptor(Name, CallType)}}
                                end
                        end
                end
        end
    catch
        error:badarg:Stack ->
            {error, {undefined_channel, Stack}}
    end.

-spec interceptor(name(), unary | stream) -> grpcbox_client:interceptor() | undefined.
interceptor(Name, CallType) ->
    case ets:lookup(?CHANNELS_TAB, {Name, CallType}) of
        [] ->
            undefined;
        [{_, I}] ->
            I
    end.

stop(Name) ->
    gen_statem:stop(?CHANNEL(Name)).

init([Name, Endpoints0, Options]) ->
    process_flag(trap_exit, true),

    Endpoints = lists:map(fun({Transport, Host, Port, SSLOptions}) ->
                                  %% legacy endpoints have 1 worker
                                  {Transport, Host, Port, SSLOptions, 1};
                             ({Transport, Host, Port, SSLOptions, Count}) ->
                                  {Transport, Host, Port, SSLOptions, Count}
                          end, Endpoints0),

    WorkerCount = lists:sum([Count || {_, _, _, _, Count} <- Endpoints ]),

    BalancerType = maps:get(balancer, Options, round_robin),
    Encoding = maps:get(encoding, Options, identity),
    StatsHandler = maps:get(stats_handler, Options, undefined),

    insert_interceptors(Name, Options),

    gproc_pool:new(Name, BalancerType, [{size, WorkerCount},
                                        {auto_size, true}]),
    Data = #data{
        pool = Name,
        encoding = Encoding,
        stats_handler = StatsHandler,
        endpoints = Endpoints
    },

    case maps:get(sync_start, Options, false) of
        false ->
            {ok, idle, Data, [{next_event, internal, connect}]};
        true ->
            _ = start_workers(Name, StatsHandler, Encoding, Endpoints),
            {ok, connected, Data}
    end.

callback_mode() ->
    state_functions.

connected({call, From}, is_ready, _Data) ->
    {keep_state_and_data, [{reply, From, true}]};
connected(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

idle(internal, connect, Data=#data{pool=Pool,
                                   stats_handler=StatsHandler,
                                   encoding=Encoding,
                                   endpoints=Endpoints}) ->
    _ = start_workers(Pool, StatsHandler, Encoding, Endpoints),
    {next_state, connected, Data};
idle({call, From}, is_ready, _Data) ->
    {keep_state_and_data, [{reply, From, false}]};
idle(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

handle_event(_, _, Data) ->
    {keep_state, Data}.

terminate(_Reason, _State, #data{pool=Name}) ->
    gproc_pool:force_delete(Name),
    ok.

insert_interceptors(Name, Interceptors) ->
    insert_unary_interceptor(Name, Interceptors),
    insert_stream_interceptor(Name, stream_interceptor, Interceptors).

insert_unary_interceptor(Name, Interceptors) ->
    case maps:get(unary_interceptor, Interceptors, undefined) of
        undefined ->
            ok;
        {Interceptor, Arg} ->
            ets:insert(?CHANNELS_TAB, {{Name, unary}, Interceptor(Arg)});
        Interceptor ->
            ets:insert(?CHANNELS_TAB, {{Name, unary}, Interceptor})
    end.

insert_stream_interceptor(Name, _Type, Interceptors) ->
    case maps:get(stream_interceptor, Interceptors, undefined) of
        undefined ->
            ok;
        {Interceptor, Arg} ->
            ets:insert(?CHANNELS_TAB, {{Name, stream}, Interceptor(Arg)});
        Interceptor when is_atom(Interceptor) ->
            ets:insert(?CHANNELS_TAB, {{Name, stream}, #{new_stream => fun Interceptor:new_stream/6,
                                                         send_msg => fun Interceptor:send_msg/3,
                                                         recv_msg => fun Interceptor:recv_msg/3}});
        Interceptor=#{new_stream := _,
                      send_msg := _,
                      recv_msg := _} ->
            ets:insert(?CHANNELS_TAB, {{Name, stream}, Interceptor})
    end.

start_workers(Pool, StatsHandler, Encoding, Endpoints) ->
    [lists:foreach(
       fun(I) ->
               Name = {Transport, Host, Port, SSLOptions, I},
               gproc_pool:add_worker(Pool, Name),
               {ok, Pid} = grpcbox_subchannel:start_link(Name, Pool,
                                                         {Transport, Host, Port, SSLOptions},
                                                         Encoding, StatsHandler),
               Pid
       end,
       lists:seq(1, Count)) || {Transport, Host, Port, SSLOptions, Count} <- Endpoints].

