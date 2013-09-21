% Copyright (c) 2001-2004 Kungliga Tekniska Högskolan
% See the file COPYING

% Usage:
%
%
% Call this before any call to fetch/1
%
% start(Host, User, Password, Database)
%
%
% Makes a database query
%
% fetch("select * from hello") -> Result
% Result = {Fieldinfo, Rows}
% Rows = [Row]
% Row = [string()]

-module(mysql).
-export([start/4, fetch/1]).

-define(LONG_PASSWORD, 1).
-define(LONG_FLAG, 4).
-define(TRANSACTIONS, 8192).
-define(INTERACTIVE, 1024).
-define(LOCAL_FILES, 128).

startrecv(Host, Port) ->
    spawn(fun () ->
                recvprocess(Host, Port, self())
        end),
    receive
        {mysql, sendsocket, Socket} ->
            Socket
    end.

recvprocess(Host, Port, Parent) ->
    {ok, Sock} = gen_tcp:connect(Host, Port, [binary, {packet, 0}]),
    Parent ! {mysql, sendsocket, Sock},
    recvloop(Sock, Parent, <<>>),
    Parent ! {mysql, closed}.

sendpacket(Parent, Data) ->
    case Data of
        <<Length:24/little, Num:8, D/binary>> ->
            if
                Length =< size(D) ->
                    {Packet, Rest} = split_binary(D, Length),
                    Parent ! {mysql, response, Packet, Num},
                    sendpacket(Parent, Rest);
                true ->
                    Data
            end;
        _ ->
            Data
    end.

recvloop(Sock, Parent, Data) ->
    Rest = sendpacket(Parent, Data),
    receive
        {tcp, Sock, InData} ->
            NewData = list_to_binary([Rest, InData]),
            recvloop(Sock, Parent, NewData);
        {tcp_error, Sock, _Reason} ->
            true;
        {tcp_closed, Sock} ->
            true
    end.

do_recv() ->
    receive
        {mysql, response, Packet, Num} ->
            {Packet, Num}
    end.

do_send(Sock, Packet, Num) ->
    Data = <<(size(Packet)):24/little, Num:8, Packet/binary>>,
    gen_tcp:send(Sock, Data).

make_auth(User, Password) ->
    Caps = ?LONG_PASSWORD bor ?LONG_FLAG bor ?TRANSACTIONS,
    Maxsize = 0,
    UserB = list_to_binary(User),
    PasswordB = list_to_binary(Password),
    <<Caps:16/little, Maxsize:24/little, UserB/binary, 0:8,
      PasswordB/binary>>.

hash(S) ->
    hash(S, 1345345333, 305419889, 7).

hash([C | S], N1, N2, Add) ->
    N1_1 = N1 bxor (((N1 band 63) + Add) * C + N1 * 256),
    N2_1 = N2 + ((N2 * 256) bxor N1_1),
    Add_1 = Add + C,
    hash(S, N1_1, N2_1, Add_1);
hash([], N1, N2, _Add) ->
    Mask = (1 bsl 31) - 1,
    {N1 band Mask , N2 band Mask}.

rnd(N, Seed1, Seed2) ->
    Mod = (1 bsl 30) - 1,
    rnd(N, [], Seed1 rem Mod, Seed2 rem Mod).

rnd(0, List, _, _) ->
    lists:reverse(List);
rnd(N, List, Seed1, Seed2) ->
    Mod = (1 bsl 30) - 1,
    NSeed1 = (Seed1 * 3 + Seed2) rem Mod,
    NSeed2 = (NSeed1 + Seed2 + 33) rem Mod,
    Float = (float(NSeed1) / float(Mod))*31,
    Val = trunc(Float)+64,
    rnd(N - 1, [Val | List], NSeed1, NSeed2).

password(Password, Salt) ->
    {P1, P2} = hash(Password),
    {S1, S2} = hash(Salt),
    Seed1 = P1 bxor S1,
    Seed2 = P2 bxor S2,
    List = rnd(9, Seed1, Seed2),
    {L, [Extra]} = lists:split(8, List),
    lists:map(fun (E) ->
                E bxor (Extra - 64)
        end, L).

asciz(Data) ->
    {String, [0 | Rest]} = lists:splitwith(fun (C) ->
                    C /= 0
            end, Data),
    {String, Rest}.

greeting(Packet) ->
    <<_Protocol:8, Rest/binary>> = Packet,
    {_Version, Rest2} = asciz(binary_to_list(Rest)),
    [_T1, _T2, _T3, _T4 | Rest3] = Rest2,
    {Salt, _Rest4} = asciz(Rest3),
    Salt.

get_with_length(<<251:8, Rest/binary>>) ->
    {null, Rest};
get_with_length(<<252:8, Length:16/little, Rest/binary>>) ->
    split_binary(Rest, Length);
get_with_length(<<253:8, Length:24/little, Rest/binary>>) ->
    split_binary(Rest, Length);
get_with_length(<<254:8, Length:64/little, Rest/binary>>) ->
    split_binary(Rest, Length);
get_with_length(<<Length:8, Rest/binary>>) when Length < 251 ->
    split_binary(Rest, Length).

get_fields() ->
    {Packet, _Num} = do_recv(),
    case Packet of
        <<254:8>> ->
            [];
        _ ->
            {Table, Rest} = get_with_length(Packet),
            {Field, Rest2} = get_with_length(Rest),
            {LengthB, Rest3} = get_with_length(Rest2),
            LengthL = size(LengthB) * 8,
            <<Length:LengthL/little>> = LengthB,
            {Type, Rest4} = get_with_length(Rest3),
            {_Flags, _Rest5} = get_with_length(Rest4),
            [{binary_to_list(Table),
              binary_to_list(Field),
              Length,
              binary_to_list(Type)} | get_fields()]
    end.

get_row(0, _Data) ->
    [];
get_row(N, Data) ->
    {Col, Rest} = get_with_length(Data),
    [case Col of
            null ->
                null;
            _ ->
                binary_to_list(Col)
        end | get_row(N - 1, Rest)].

get_rows(N) ->
    {Packet, _Num} = do_recv(),
    case Packet of
        <<254:8>> ->
            [];
        _ ->
            [get_row(N, Packet) | get_rows(N)]
    end.

get_query_response() ->
    {<<Fieldcount:8, Rest/binary>>, _} = do_recv(),
    case Fieldcount of
        0 ->
            {[], []};
        255 ->
            <<_Code:16/little, Message/binary>>  = Rest,
            {error, binary_to_list(Message)};
        _ ->
            Fields = get_fields(),
            Rows = get_rows(Fieldcount),
            {Fields, Rows}
    end.

do_query(Sock, Query) ->
    Q = list_to_binary(Query),
    Packet = <<3, Q/binary>>,
    do_send(Sock, Packet, 0),
    get_query_response().

do_init(Sock, User, Password) ->
    {Packet, _Num} = do_recv(),
    Salt = greeting(Packet),
    Auth = password(Password, Salt),
    Packet2 = make_auth(User, Auth),
    do_send(Sock, Packet2, 1),
    {Packet3, _Num3} = do_recv(),
    case Packet3 of
        <<0:8, _Rest/binary>> ->
            ok;
        <<255:8, Code:16/little, Message/binary>> ->
            io:format("Error ~p: ~p", [Code, binary_to_list(Message)]),
            error;
        _ ->
            io:format("Unknown error ~p", [binary_to_list(Packet3)]),
            error
    end.

connect(Host, User, Password, Database) ->
    connect(Host, User, Password, Database, closed).

connect(Host, User, Password, Database, closed) ->
    receive
        {mysql, fetchquery, Query, Pid} ->
            Sock = startrecv(Host, 3306),
            case do_init(Sock, User, Password) of
                ok ->
                    do_query(Sock, "use " ++ Database),
                    Result = do_query(Sock, Query),
                    Pid ! {mysql, result, Result},
                    connect(Host, User, Password, Database, {open, Sock});
                _ ->
                    gen_tcp:close(Sock),
                    connect(Host, User, Password, Database, closed)
            end
    end;
connect(Host, User, Password, Database, {open, Sock}) ->
    receive
        {mysql, fetchquery, Query, Pid} ->
            Result = do_query(Sock, Query),
            Pid ! {mysql, result, Result},
            connect(Host, User, Password, Database, {open, Sock});
        {mysql, closed} ->
            connect(Host, User, Password, Database, closed)
    end.

start(Host, User, Password, Database) ->
    Pid = spawn(fun () ->
                    connect(Host, User, Password, Database)
            end),
    register(mysql, Pid).

fetch(Query) ->
    mysql ! {mysql, fetchquery, Query, self()},
    receive
        {mysql, result, Result} ->
            Result
    after
        1000 ->
            none
    end.
