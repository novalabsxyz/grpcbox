-module(grpcbox_pool).
-include("grpcbox.hrl").

-behaviour(acceptor_pool).

-export([start_link/4,
         accept_socket/3,
         pool_sockets/1,
         connection_count/1,
         inc_conn_count/1,
         dec_conn_count/1]).

-export([init/1]).

start_link(PoolName, ServerOpts, ChatterboxOpts, TransportOpts) ->
    acceptor_pool:start_link({local, PoolName}, PoolName, ?MODULE,
        [PoolName, ServerOpts, ChatterboxOpts, TransportOpts]).

accept_socket(Pool, Socket, Acceptors) ->
    acceptor_pool:accept_socket(Pool, Socket, Acceptors).

pool_sockets(Pool) ->
    acceptor_pool:which_sockets(Pool).

%% return the number of active connection for the specified poolname
connection_count(Pool) ->
    case ets:lookup(?ACTIVE_CONNS_PER_POOL_TABLE, Pool) of
        [{_, N}] -> N;
        _ -> 0
    end.

%% inc the count of active connections for the specified poolname
%% update_counter(Tab, Key, {Pos, Inc, Threshold, SetValue}, Default)
inc_conn_count(Pool) ->
    ets:update_counter(?ACTIVE_CONNS_PER_POOL_TABLE, Pool, {2, 1}, {Pool, 0}).

%% dec the count of active connections for the specified poolname
%% dont allow the count to drop below threshold of zero
dec_conn_count(Pool) ->
    ets:update_counter(?ACTIVE_CONNS_PER_POOL_TABLE, Pool, {2, -1, 0, 0}).

init([PoolName, ServerOpts, ChatterboxOpts, TransportOpts]) ->
    {Transport, SslOpts} = case TransportOpts of
                               #{ssl := true,
                                 keyfile := KeyFile,
                                 certfile := CertFile,
                                 cacertfile := CACertFile} ->
                                   {ssl, [{keyfile, KeyFile},
                                          {certfile, CertFile},
                                          {honor_cipher_order, false},
                                          {cacertfile, CACertFile},
                                          {fail_if_no_peer_cert, true},
                                          {verify, verify_peer},
                                          {versions, ['tlsv1.2']},
                                          {next_protocols_advertised, [<<"h2">>]}]};
                               _ ->
                                   {gen_tcp, []}
                           end,
    Conn = #{id => grpcbox_acceptor,
             start => {grpcbox_acceptor, {PoolName, Transport, ServerOpts, ChatterboxOpts, SslOpts}, []},
             grace => 5000},
    {ok, {#{intensity => 50, period => 2}, [Conn]}}.


