#!/usr/bin/env bash

set -euo pipefail

MAIN_CLASS=io.aeron.samples.echo.ProvisioningServerMain

function start_server()
{
  rm -f nohup.out

  parse_version || java_version=${?}
  echo ${java_version}
  if [[ ${java_version} -le 8 ]]; then
    ADD_OPENS=''
  else
    ADD_OPENS='--add-opens java.base/sun.nio.ch=ALL-UNNAMED'
  fi

  nohup ${JAVA_HOME}/bin/java \
    -Dcom.sun.management.jmxremote \
    -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.ssl=false \
    -Dcom.sun.management.jmxremote.port=10000 \
    -Djava.rmi.server.hostname=${PROVISIONING_HOST} \
    ${ADD_OPENS} \
    -cp ${AERON_HOME}/aeron-all-1.38.0-SNAPSHOT.jar \
    ${MAIN_CLASS} \
    &
}

function stop_server()
{
  pkill -f ${MAIN_CLASS} || true
}

function status_server()
{
  pgrep -f ${MAIN_CLASS} || exit_code=$?
  if [ -z ${exit_code+x} ]; then
    echo "OK"
  else
    echo "Not running:"
    cat ${AERON_HOME}/log
  fi
}

function parse_version()
{
  java_version_str=$(${JAVA_HOME}/bin/java -version 2>&1 | awk -F '"' '/version/ {print $2}')
  if [[ ${java_version_str} =~ (.*)\.(.*)\. ]]; then
    if [[ ${BASH_REMATCH[1]} = "1" ]]; then
      return ${BASH_REMATCH[2]}
    else
      return ${BASH_REMATCH[1]}
    fi
  else
    echo "Invalid version string ${java_version_str}"
    exit 1
  fi
}

function version_server()
{
  parse_version || java_version=${?}
  echo ${java_version}
  if [[ ${java_version} -le 8 ]]; then
    ADD_OPENS=""
  else
    ADD_OPENS="--add-opens 'java.base/sun.nio.ch=ALL-UNNAMED'"
  fi
  echo ${ADD_OPENS}
}

while [[ $# -gt 0 ]]
do
  option="${1}"
  case ${option} in
    start)
      start_server
      shift
      ;;
    stop)
      stop_server
      shift
      ;;
    status)
      status_server
      shift
      ;;
    version)
      version_server
      shift
      ;;
    -h|--help)
      echo "provisioning_server.sh <start|stop>"
      shift
      ;;
    *)
      echo "Unknown option ${option}"
      echo "Use --help for help"
      exit
      ;;
  esac
done
