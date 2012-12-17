#!/bin/bash

rm -r `find . -name *.pyc -print`
rm -r build saga.egg-info temp 

SAGA_VERBBOSE=DEBUG nosetests -v \
 ./saga/engine/config.py \
 ./saga/engine/logger.py \
 ./saga/engine/engine.py \
 ./saga/utils/which.py

