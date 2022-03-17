#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

# Clear any cached results
find . -name "*.pyc" -exec rm -f {} \;

echo "Running tests"
coverage run --source=./gobimport -m pytest tests/

echo "Running coverage report"
coverage report --show-missing --fail-under=95

echo "Running style checks"
flake8
