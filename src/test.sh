#!/usr/bin/env bash
set -e
set -u

export COVERAGE_FILE="/tmp/.coverage"

echo "Running mypy"
mypy gobimport

echo "Running unit tests"
coverage run -m pytest

echo "Reporting coverage"
coverage report --fail-under=96

echo "Check if black finds no potential reformat fixes"
black --check gobimport

echo "Check for potential import sort"
isort --check --diff gobimport

echo "Running flake8"
flake8 gobimport

echo "Checks complete"
