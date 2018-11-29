#!/usr/bin/env bash

set -e # stop on any error

# Start from directory where this script is located
SCRIPTDIR="$( cd "$( dirname "$0" )" >/dev/null && pwd )"

if [ -z "$1" ]; then
    echo "Usage: bash run_test.sh test-id"
    echo "Example: bash run_test.sh ADD"
    exit 1
fi

TEST=$1

if [ -f "${SCRIPTDIR}/test.csv" ]; then
    rm ${SCRIPTDIR}/test.csv
fi
cp ${SCRIPTDIR}/test_${TEST}.csv ${SCRIPTDIR}/test.csv
