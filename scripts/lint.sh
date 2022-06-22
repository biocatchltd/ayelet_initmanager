#!/bin/sh
set -e
echo "running isort..."
python -m isort app tests --check-only
echo "running flake8..."
python -m flake8 app/ tests/
echo "running mypy..."
python -m mypy --show-error-codes app