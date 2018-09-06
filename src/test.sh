#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

echo "Running style checks"
flake8

echo "Running unit tests - MISSING!"
python -m unittest

echo "Running coverage tests - MISSING!"
export COVERAGE_FILE=/tmp/.coverage
coverage erase
coverage run -m unittest
coverage report --fail-under=0 gobimportclient/*.py
