set -e

coverage run --branch --context=blackbox --include="app/*" -m pytest tests/blackbox/app
coverage report -m
coverage html
coverage xml