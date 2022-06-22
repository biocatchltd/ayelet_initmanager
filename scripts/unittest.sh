#!/bin/sh
set -e

coverage run --branch --context=ut --include="app/*" -m pytest tests/unittest
coverage report -m
coverage html
coverage xml