cd /d "%~dp0"

set "AERON_TERM_BUFFER_SPARSE_FILE=0"
set "AERON_MTU_LENGTH=60000"
set "AERON_RCV_INITIAL_WINDOW_LENGTH=2m"
set "AERON_THREADING_MODE=DEDICATED"
set "AERON_CONDUCTOR_IDLE_STRATEGY=spin"
set "AERON_SENDER_IDLE_STRATEGY=noop"
set "AERON_RECEIVER_IDLE_STRATEGY=noop"
::-Daeron.dir=R:\Temp\aeron
aeronmd -Daeron.print.configuration=true -Daeron.socket.so_sndbuf=2m -Daeron.socket.so_rcvbuf=2m  ^
 -Daeron.dir.delete.on.start=1 -Daeron.dir.delete.on.shutdown=1 ^
 -Daeron.term.buffer.length=67108864

pause
