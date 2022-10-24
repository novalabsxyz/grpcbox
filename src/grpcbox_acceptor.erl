-module(grpcbox_acceptor).

-behaviour(acceptor).

-export([acceptor_init/3,
         acceptor_continue/3,
         acceptor_terminate/2]).

acceptor_init(_, LSocket, {PoolName, Transport, ServerOpts, ChatterboxOpts, SslOpts}) ->
    % monitor listen socket to gracefully close when it closes
    MRef = monitor(port, LSocket),
    {ok, {Transport, MRef, PoolName, ServerOpts, ChatterboxOpts, SslOpts}}.

acceptor_continue(_PeerName, Socket, {ssl, _MRef, PoolName, ServerOpts, ChatterboxOpts, SslOpts}) ->
    {ok, AcceptSocket} = ssl:handshake(Socket, SslOpts),
    case ssl:negotiated_protocol(AcceptSocket) of
        {ok, <<"h2">>} ->
            %% get the max connection settings from the grpcbox config
            %% default to unlimited if not specified
            MaxConns = maps:get(max_connections, ServerOpts, unlimited),
            case connection_allowed(PoolName, MaxConns) of
                true ->
                    h2_connection:become({ssl, AcceptSocket}, chatterbox:settings(server, ServerOpts), ChatterboxOpts),
                    _ = grpcbox_pool:inc_conn_count(PoolName);
                false ->
                    exit(max_connections_exceeded)
            end;
        _ ->
            exit(bad_negotiated_protocol)
    end;

acceptor_continue(_PeerName, Socket, {gen_tcp, _MRef, PoolName, ServerOpts, ChatterboxOpts, _SslOpts}) ->
    MaxConns = maps:get(max_connections, ServerOpts, unlimited),
    case connection_allowed(PoolName, MaxConns) of
        true ->
            h2_connection:become({gen_tcp, Socket}, chatterbox:settings(server, ServerOpts), ChatterboxOpts),
            _ = grpcbox_pool:inc_conn_count(PoolName);
        false ->
            exit(max_connections_exceeded)
    end.

acceptor_terminate(Reason, {_, _MRef, PoolName, _ServerOpts, _ChatterboxOpts, _SslOpts}) ->
    % Something went wrong. Either the acceptor_pool is terminating or the
    % accept failed.
    _ = grpcbox_pool:dec_conn_count(PoolName),
    exit(Reason).

connection_allowed(_PoolName, unlimited) ->
    true;
connection_allowed(PoolName, MaxConns) ->
    grpcbox_pool:connection_count(PoolName) =< MaxConns.
