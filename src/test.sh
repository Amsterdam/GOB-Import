#!/usr/bin/env bash
set -e
set -u

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
#  "gobimport/converter.py"
  "gobimport/__init__.py"
#  "gobimport/import_client.py"
#  "gobimport/reader.py"
#  "gobimport/utils.py"
#  "gobimport/injections.py"
#  "gobimport/__main__.py"
#  "gobimport/merger.py"
)

echo "Running mypy"
mypy "${FILES[@]}"

echo "Running unit tests"
coverage run --source=gobimport -m pytest

echo "Reporting coverage"
coverage report --fail-under=96

echo "Check if black finds no potential reformat fixes"
black --verbose --check "${FILES[@]}"

echo "Check for potential import sort"
isort --check --diff "${FILES[@]}"

echo "Running flake8"
flake8 "${FILES[@]}"

echo "Checks complete"
