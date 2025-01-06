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

set numArgs=0
for %%x in (%*) do set /A numArgs+=1

if %numArgs% LSS 2 (
  echo "Usage: <PID> <command> [config files...]"
  echo "  <PID> - Java process ID to attach logging agent to."
  echo "  <command> - either 'start' to start logging or 'stop' to stop it."
  echo "  [config files...] - an optional list of property files to configure logging options."
  echo "Alternatively logging options can be specified via the 'JVM_OPTS' env variable, e.g.:"
  echo "set \"JVM_OPTS=-Daeron.event.log=admin -Daeron.event.archive.log=all\" && dynamic-logging 1111 start"
  exit /b 1
)

call "%DIR%\java-common"

set "AGENT_JAR=%DIR%\..\..\aeron-agent\build\libs\aeron-agent-%VERSION%.jar"

"%JAVA_HOME%\bin\java" ^
  -cp "%AGENT_JAR%" ^
  !JAVA_OPTIONS! ^
  !ADD_OPENS! ^
  %JVM_OPTS% ^
  io.aeron.agent.DynamicLoggingAgent ^
  %AGENT_JAR% ^
  %*