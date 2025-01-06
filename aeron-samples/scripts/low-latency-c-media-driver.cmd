::
:: Copyright 2014-2025 Real Logic Limited.
::
:: Licensed under the Apache License, Version 2.0 (the "License");
:: you may not use this file except in compliance with the License.
:: You may obtain a copy of the License at
::
:: https://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.
::

@echo off
set "DIR=%~dp0"

set "AERON_BUILD_DIR=%DIR%\..\..\cppbuild\Release"

set "AERON_TERM_BUFFER_SPARSE_FILE=0"
set "AERON_SOCKET_SO_RCVBUF=2m"
set "AERON_SOCKET_SO_SNDBUF=2m"
set "AERON_RCV_INITIAL_WINDOW_LENGTH=2m"
set "AERON_THREADING_MODE=DEDICATED"
set "AERON_CONDUCTOR_IDLE_STRATEGY=spin"
set "AERON_SENDER_IDLE_STRATEGY=noop"
set "AERON_RECEIVER_IDLE_STRATEGY=noop"
set "AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND=2"
set "AERON_PRINT_CONFIGURATION=1"

"%AERON_BUILD_DIR%\binaries\Release\aeronmd.exe"
