#!/bin/bash
docker run -v `pwd`:/opal --rm python:3.9 bash -c "pip install /opal/opal-packages pytest && pytest /opal"
