#!/bin/bash

pipenv run pipenv shell
pipenv run pipenv install

# set python path to project dir
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
