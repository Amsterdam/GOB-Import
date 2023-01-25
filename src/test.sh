#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

echo() {
   builtin echo -e "$@"
}

export COVERAGE_FILE="/tmp/.coverage"

# Uncomment files to pass through checks
FILES=(
#  "gobimport/enricher/bag.py"
#  "gobimport/enricher/meetbouten.py"
#  "gobimport/enricher/test_catalogue.py"
#  "gobimport/enricher/__init__.py"
#  "gobimport/enricher/enricher.py"
#  "gobimport/enricher/gebieden.py"
#  "gobimport/entity_validator/bag.py"
#  "gobimport/entity_validator/__init__.py"
#  "gobimport/entity_validator/gebieden.py"
#  "gobimport/entity_validator/state.py"
#  "gobimport/validator/config.py"
#  "gobimport/validator/__init__.py"
  "gobimport/converter.py"
  "gobimport/__init__.py"
#  "gobimport/import_client.py"
#  "gobimport/reader.py"
  "gobimport/utils.py"
#  "gobimport/injections.py"
#  "gobimport/__main__.py"
#  "gobimport/merger.py"
)

echo "Running mypy"
mypy "${FILES[@]}"

echo "\nRunning unit tests"
coverage run --source=gobimport -m pytest

echo "Coverage report"
coverage report --fail-under=96

echo "\nCheck if Black finds no potential reformat fixes"
black --check --diff "${FILES[@]}"

echo "\nCheck for potential import sort"
isort --check --diff "${FILES[@]}"

echo "\nRunning Flake8 style checks"
flake8 "${FILES[@]}"

echo "\nChecks complete"
