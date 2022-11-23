#!/bin/bash

TEST_ENV="${TEST_ENV:-}"
OPAL_ROOT=$(dirname $0)/..

ARGS=$(getopt -o "ahe:o:" \
    --long acceptance,environment:,help,opal-root: \
    -n test_all \
    -- "$@" )

show_help() {
    cat << EOF
usage: 
    $0 [-a|--acceptance] 
       [-o|--opal-root OPAL_ROOT]
       [-e|--environment TEST_ENVIRONMENT_NAME]
       [-h|--help]
    
    Run all automated tests for the OPAL framework.  Optional tests can be 
    enabled if a TEST_ENVIRONEMNT_NAME is specified as the test environment. 
    The only currently supported test environment is "acceptance".
    
    -e|--environment   set testing enviroment to TEST_ENVIRONMENT_NAME
    -a|--acceptance    set testing environment to acceptance. This is equivalent
                       to passing "-e acceptance".
    -o|--opal-root     set directory where opal project is installed
    -h|--help          show this help
    
    TEST_ENVIRONMENT_NAME will default to the environment variable TEST_ENV if
    it is set.  The command line options will be used first, if specified.  
    If multiple values on the command line are specified, the last will be 
    used.
    
    OPAL_ROOT will default to one directory up from the present working directory.
EOF
}


eval set -- "${ARGS}"
while true ; do
case "$1" in
    -h | --help )
        show_help
        exit 0
        ;;
        
    -o | --opal-root )
        OPAL_ROOT=$2
        shift 2
        ;;

    -e | --environment )
        TEST_ENV=$2
        shift 2
        ;;


    -a | --acceptance )
        TEST_ENV=acceptance
        shift
        ;;
        
    -- )
        shift
        break
        ;;
esac
done

TEST_ENV=${TEST_ENV,,}

echo "OPAL_ROOT: ${OPAL_ROOT}"

echo "test environment: $TEST_ENV"
if [[ ${TEST_ENV} == "acceptance" ]] ; then
    echo "acceptance only scripts"
fi

echo "-----------------------------------------"
echo "-------------- tip tests ----------------"
echo "-----------------------------------------"
tests

echo "-----------------------------------------"
echo "---------------- pytest -----------------"
echo "-----------------------------------------"

pytest $OPAL_ROOT/opal-packages

echo "-----------------------------------------"
echo "------------- test notebooks ------------"
echo "-----------------------------------------"

# this generates a lot of extra output
jupyter nbconvert --execute $OPAL_ROOT/test-notebooks/*.ipynb \
--to notebook --inplace

if [[ ${TEST_ENV} == "acceptance" ]] ; then
    echo "-----------------------------------------"
    echo "----------- test demo notebooks ---------"
    echo "-----------------------------------------"
    TEST_ENV=$TEST_ENV jupyter nbconvert --execute \
    $OPAL_ROOT/demo-notebooks/*.ipynb --to notebook --inplace
fi

if [[ $? -ne 0 ]]; then
    echo "A notebook failed!"
    exit 1
else
    echo "Notebooks Passed"
fi

echo "-----------------------------------------"
echo "-------- default environment test -------"
echo "-----------------------------------------"

[ $CONDA_DEFAULT_ENV == singleuser ] || \
(echo "Default env \"$CONDA_DEFAULT_ENV\" is not singleuser"; exit 1)