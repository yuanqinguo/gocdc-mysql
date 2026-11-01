#!/bin/bash
P=$(ps -eo pid,args | grep "10_boot.sh" | grep -v grep | awk '{print $1}' | head -1)
echo "script pid=$P"
ps --ppid $P -o pid,etime,args 2>/dev/null
echo "--- 子进程树（含孙进程）"
CPS=$(ps --ppid $P -o pid= 2>/dev/null)
for c in $CPS; do ps --ppid $c -o pid,etime,args 2>/dev/null; done
echo "--- wchan/syscall"
cat /proc/$P/wchan 2>/dev/null; echo
echo "--- rmq 状态"
docker ps --format '{{.Names}}\t{{.Status}}' | grep rmq
docker exec rmqbroker sh -c 'netstat -ltn 2>/dev/null | grep -E "8081|10911" || ss -ltn 2>/dev/null | grep -E "8081|10911"' 2>/dev/null | head -5
echo "--- WSL 8081 可达性"
ss -ltn | grep 8081 || echo "WSL 无 8081 监听"
timeout 3 bash -c 'echo > /dev/tcp/127.0.0.1/8081' 2>&1 && echo "8081 TCP 可达" || echo "8081 TCP 不可达"
