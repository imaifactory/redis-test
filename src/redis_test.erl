-module(redis_test).

-define(PRECISION,10).

-export([start/1]).

start([WriteProcessSrc,ReadProcessSrc, CountSrc, Redis]) ->
    {ok, Pid} = eredis:start_link(Redis,6379),    
    eredis:q(Pid,["flushall"]),

    {WriteProcess,_} = string:to_integer(WriteProcessSrc),
    {ReadProcess,_}  = string:to_integer(ReadProcessSrc),
    {Count,_}        = string:to_integer(CountSrc),

    parallel_set(WriteProcess, Count, Pid),
    parallel_get(ReadProcess, Count, Pid),
    ProcessCount = WriteProcess + ReadProcess,
    collect_loop(ProcessCount).

   
get_seconds() ->
    {Mega, Sec, Micro} = now(),
    ((Mega * 1000000 + Sec) * 1000000 + Micro) / 1000000.
    

parallel_get(0, _Count, _Pid) -> ok;
parallel_get(ProcessCount, Count, Pid) ->
    ParentPid = self(),
    spawn(fun() ->
                get_key(Count, Pid),
                ParentPid ! ok
          end),
    parallel_get(ProcessCount - 1, Count, Pid).

get_key(0, _Pid) -> ok;
get_key(Count,Pid) ->
    StartTime = get_seconds(),
    eredis:q(Pid,['get'|[random:uniform(750000000),"1"]]),
    EndTime = get_seconds(),
    Flag = judge(Count),
    output("Read",Flag, EndTime - StartTime),
    get_key(Count -1, Pid).


parallel_set(0, _Count, _Pid) -> ok;
parallel_set(ProcessCount, Count, Pid) ->
    ParentPid = self(),
    spawn(fun() ->
                set_key(Count, Pid),
                ParentPid ! ok
          end),
    parallel_set(ProcessCount - 1, Count, Pid).

set_key(0, _Pid) -> ok;
set_key(Count,Pid) ->
    StartTime = get_seconds(),
    eredis:q(Pid,['set'|[random:uniform(750000000),"1"]]),
    EndTime = get_seconds(),
    Flag = judge(Count),
    output("Write",Flag, EndTime - StartTime),
    set_key(Count -1, Pid).

judge(Count) -> Count rem ?PRECISION.

output(Operation,0,Time) ->
    {{Year, Month, Day}, {Hour, Min, _Sec}} =  calendar:local_time(),
    catch io:format("~w-~w-~w ~w:~w,~s,~w~n",[Year,Month,Day,Hour,Min,Operation,Time * 1000]);
output(_Operation,_Count,_Time) -> ok.
    
    
collect_loop(0) -> ok;
collect_loop(Count) ->
    receive
        ok -> collect_loop(Count - 1)
    end.
