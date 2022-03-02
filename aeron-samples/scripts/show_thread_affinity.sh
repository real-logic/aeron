#!/usr/bin/env bash

set -euo pipefail

pname=${1:-aeronmd}  # default to 'aeronmd'

for pid in $(pgrep "${pname}"); do
    echo "PID: ${pid} (${pname})"
    ps -T -p ${pid} -o spid,comm | while read line; do
        if [[ ${line} != *"SPID"* ]]; then
            IFS=" " read -a fields<<<"${line}"
            aff=$(taskset -cp "${fields[0]}" | tr -d '\n')
            printf "%s (%s)\n" "${aff}" "${fields[1]}"
        fi
    done
done

