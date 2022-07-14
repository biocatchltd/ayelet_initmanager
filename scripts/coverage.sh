#!/bin/sh
set -e

coverage run --branch --context=ut --include="app/*" -m pytest tests/unittest
coverage run -a --branch --context=blackbox --include="ayelet_initmanager/*" -m pytest tests/blackbox/app
coverage report -m
coverage html
coverage xml