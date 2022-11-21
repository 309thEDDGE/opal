#!/bin/sh
docker build -t etl_utils_test_container .
docker run --rm -it  etl_utils_test_container