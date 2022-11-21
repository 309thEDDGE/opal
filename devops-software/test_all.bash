#!/bin/bash

OPAL_PATH=${1:-~/opal}

echo "-----------------------------------------"
echo "-------------- tip tests ----------------"
echo "-----------------------------------------"
tests

echo "-----------------------------------------"
echo "---------------- pytest -----------------"
echo "-----------------------------------------"

pytest $OPAL_PATH/opal-packages

echo "-----------------------------------------"
echo "------------- test notebooks ------------"
echo "-----------------------------------------"

# this generates a lot of extra output
jupyter nbconvert --execute $OPAL_PATH/test-notebooks/*.ipynb --to notebook --inplace

if [[ $? -ne 0 ]]; then
    echo "A notebook failed!"
    exit 1
else
    echo "Notebooks Passed"
fi

echo "-----------------------------------------"
echo "-------- default environment test -------"
echo "-----------------------------------------"

[ $CONDA_DEFAULT_ENV == singleuser ] || (echo "Default env \"$CONDA_DEFAULT_ENV\" is not singleuser"; exit 1)