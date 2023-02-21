#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

echo() {
   builtin echo -e "$@"
}

export COVERAGE_FILE="/tmp/.coverage"


echo "Running mypy"
mypy gobimport

echo "\nRunning unit tests"
coverage run --source=gobimport -m pytest

echo "Coverage report"
coverage report --fail-under=96

echo "\nCheck if Black finds potential reformat fixes"
black --check --diff gobimport

echo "\nCheck for potential import sort"
isort --check --diff --src-path=gobimport gobimport

echo "\nRunning Flake8 style checks"
flake8 gobimport

echo "\nChecks complete"
