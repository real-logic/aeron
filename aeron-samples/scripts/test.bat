basic-publisher "aeron:udp?endpoint=224.0.1.1:40456|interface=192.168.199.0/24" 1001

basic-subscriber "aeron:udp?endpoint=224.0.1.1:40456|interface=192.168.199.0/24" 1001



cd ~/shared/work/aeron/aeron/aeron-samples/scripts && ./low-latency-c-media-driver

cd ~/shared/work/aeron/aeron/aeron-samples/scripts && JAVA_HOME=/usr ./low-latency-media-driver

MultiCast

export JAVA_HOME=/usr
cd ~/shared/work/aeron/aeron/aeron-samples/scripts
./ping "aeron:udp?endpoint=224.0.1.1:40456|interface=192.168.199.0/24|term-length=64k" 1001 "aeron:udp?endpoint=224.0.1.1:40456|interface=192.168.199.0/24|term-length=64k" 1002 100

export JAVA_HOME=/usr
cd ~/shared/work/aeron/aeron/aeron-samples/scripts
./pong "aeron:udp?endpoint=224.0.1.1:40456|interface=192.168.199.0/24|term-length=64k" 1001 "aeron:udp?endpoint=224.0.1.1:40456|interface=192.168.199.0/24|term-length=64k" 1002

MDC

export JAVA_HOME=/usr
cd ~/shared/work/aeron/aeron/aeron-samples/scripts
./ping "aeron:udp?control=192.168.199.1:40456|control-mode=dynamic" 1001 "aeron:udp?control=192.168.199.2:40456|control-mode=dynamic" 1002 100

export JAVA_HOME=/usr
cd ~/shared/work/aeron/aeron/aeron-samples/scripts
./pong "aeron:udp?control=192.168.199.1:40456|control-mode=dynamic" 1001 "aeron:udp?control=192.168.199.2:40456|control-mode=dynamic" 1002


Unicast

export JAVA_HOME=/usr
cd ~/shared/work/aeron/aeron/aeron-samples/scripts
./ping "aeron:udp?endpoint=192.168.199.1:9238" 1001 "aeron:udp?endpoint=192.168.199.189:9238" 1002 100

export JAVA_HOME=/usr
cd ~/shared/work/aeron/aeron/aeron-samples/scripts
./pong "aeron:udp?endpoint=192.168.199.1:9238" 1001 "aeron:udp?endpoint=192.168.199.189:9238" 1002

Localhost

export JAVA_HOME=/usr
cd ~/shared/work/aeron/aeron/aeron-samples/scripts
./pong "aeron:udp?endpoint=localhost:9238|term-length=8m|reliable=true" 1001 "aeron:udp?endpoint=localhost:9239|term-length=8m|reliable=true" 1002

export JAVA_HOME=/usr
cd ~/shared/work/aeron/aeron/aeron-samples/scripts
./ping "aeron:udp?endpoint=localhost:9238|term-length=8m|reliable=true" 1001 "aeron:udp?endpoint=localhost:9239|term-length=8m|reliable=true" 1002 100


export JAVA_HOME=/usr
cd ~/shared/work/aeron/aeron/aeron-samples/scripts
./ping "aeron:udp?endpoint=localhost:9238|term-length=8m|reliable=true" 1001 "aeron:udp?endpoint=localhost:9239|term-length=8m|reliable=true" 1002 8000


Win:

cd aeron-samples/scripts
$ENV:PING_CHANNEL="aeron:udp?endpoint=localhost:9238|term-length=8m|reliable=true"
$ENV:PING_STREAM_ID=1001
$ENV:PONG_CHANNEL="aeron:udp?endpoint=localhost:9239|term-length=8m|reliable=true"
$ENV:PONG_STREAM_ID=1002

./pong.bat 

./ping.bat 100



Stress Test:

 ./Throughput  -L 8388608 -m 1000


 ./Throughput  -L  262144 -m 100000

 ./Throughput  -L  60000 -m 100000

 ./Throughput  -L  59968 -m 100000

