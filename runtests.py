#!/bin/bash

rm -r `find . -name *.pyc -print`
rm -r build saga.egg-info temp 

SAGA_VERBBOSE=DEBUG nosetests -v ./saga/core/config.py ./saga/core/logger.py ./saga/core/core.py

