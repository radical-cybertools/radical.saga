#!/bin/bash

SAGA_VERBBOSE=DEBUG nosetests -v ./saga/core/config.py ./saga/core/logger.py ./saga/core/core.py

