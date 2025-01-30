#!/usr/bin/env bash

if [ -z "$AERON_GITHUB_PAT" ]
then
  echo "Please set AERON_GITHUB_PAT environment variable to contain your token"
  exit 1
fi

event_type=run-commit-tests

for option in "$@"
do
  case ${option} in
    -s|--slow)
      event_type=run-slow-tests
      shift
      ;;
    -c|--commit)
      shift
      ;;
    *)
      echo "$0 [-s|--slow-tests] (run slow tests) [-c|--commit] (run commit tests) default: commit tests"
      exit
      ;;
  esac
done

echo "Sending repository_dispatch, event_type: ${event_type}"

curl -v -H "Accept: application/vnd.github.everest-preview+json" \
    -H "Authorization: token ${AERON_GITHUB_PAT}" \
    --request POST \
    --data "{\"event_type\": \"${event_type}\"}" \
    https://api.github.com/repos/aeron-io/aeron/dispatches