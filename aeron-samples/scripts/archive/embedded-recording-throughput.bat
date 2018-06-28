::
:: Copyright 2014-2018 Real Logic Ltd.
::
:: Licensed under the Apache License, Version 2.0 (the "License");
:: you may not use this file except in compliance with the License.
:: You may obtain a copy of the License at
::
:: http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.
::

@echo off
"%JAVA_HOME%\bin\java" ^
    -cp ..\..\build\libs\samples.jar ^
    -XX:BiasedLockingStartupDelay=0 ^
    -Dagrona.disable.bounds.checks=true ^
    -Daeron.sample.messageLength=32 ^
    -Daeron.sample.messages=100000000 ^
    -Daeron.term.buffer.sparse.file=false ^
    -Daeron.mtu.length=16384 ^
    -Daeron.socket.so_sndbuf=2m ^
    -Daeron.socket.so_rcvbuf=2m ^
    -Daeron.rcv.initial.window.length=2m ^
    -Daeron.archive.file.sync.level=0 ^
    %JVM_OPTS% io.aeron.samples.archive.EmbeddedRecordingThroughput %*