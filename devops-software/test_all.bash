#!/bin/bash

export TEST="$(readlink -e $0)"
TEST_ENV="${TEST_ENV:-}"

#these are hard coded and subject to breaking if we
#change jupyter setup
SINGLEUSER_BIN=/opt/conda/envs/singleuser/bin/python3
SINGLEUSER_ENV=conda-env-singleuser-py

DEFAULT_OPAL_ROOT="$(readlink -e "$(dirname $0)/..")"
OPAL_ROOT=${OPAL_ROOT:-${DEFAULT_OPAL_ROOT}}

#flags
VERBOSE=""
EXCLUDE_SINGLEUSER=""
EXCLUDE_PYTEST=""
EXCLUDE_OPS_PYTEST=""
EXCLUDE_TIP=""
EXCLUDE_WEAVE=""
EXCLUDE_STARTER_NOTEBOOKS=""
EXCLUDE_TEST_NOTEBOOKS=""
EXCLUDE_DEMO_NOTEBOOKS="EXCLUDE_DEMO_NOTEBOOKS"

ARGS=$(getopt -o "ahe:o:v" \
    --long "acceptance,environment:,help,opal-root:,verbose,no-singleuser," \
    --long "no-torch,no-tip,no-weave,no-pytest,no-ops-pytest,no-starter-notebooks,no-test-notebooks," \
    --long "no-demo-notebooks,no-notebooks" \
    -n test_all \
    -- "$@" )
(( $? == 0 )) || exit 1 ;


show_help() {
    cat << EOF
usage:
    $0 [-a|--acceptance]
       [-o|--opal-root OPAL_ROOT]
       [-e|--environment TEST_ENVIRONMENT_NAME]
       [--no-singleuser]
       [--no-tip]
       [--no-weave]
       [--no-pytest]
       [--no-ops-pytest]
       [--no-starter-notebooks]
       [--no-test-notebooks]
       [--no-demo-notebooks]
       [--no-notebooks]
       [-v|--verbose]
       [-h|--help]

    Run all automated tests for the OPAL framework.  Optional tests can be
    enabled if a TEST_ENVIRONEMNT_NAME is specified as the test environment.
    The only currently supported test environment is "acceptance".

    -e|--environment   set testing enviroment to TEST_ENVIRONMENT_NAME
    -a|--acceptance    set testing environment to acceptance. This is 
                       equivalent to passing "-e acceptance".
    -o|--opal-root     set directory where opal project is installed
    -v|--verbose       print backtrace for failing notebooks
    --no-singleuser    no tests run for singleuser conda environment
    --no-tip           no tests run for TIP
    --no-pytest        no pytest tests are run
    --no-ops-pytest    no devops pytests are run
    --no-starter-notebooks        do not run starter notebooks
    --no-test-notebooks           do not run test notebooks
    --no-demo-notebooks           do not run demo notebooks
    --no-notebooks     don't run any notebooks
    -h|--help          show this help

    TEST_ENVIRONMENT_NAME will default to the environment variable TEST_ENV
    if it is set.  The command line options will be used first, if specified.
    If multiple values on the command line are specified, the last will be
    used.

    OPAL_ROOT will default to one directory up from the present working
    directory.
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
        if [[ "${TEST_ENV}" == "acceptance" ]] ; then
            EXCLUDE_DEMO_NOTEBOOKS=""
        fi
        shift 2
        ;;

    -a | --acceptance )
        TEST_ENV=acceptance
        EXCLUDE_DEMO_NOTEBOOKS=""
        shift
        ;;

    -v|--verbose)
        VERBOSE="VERBOSE"
        shift
        ;;

    --no-singleuser)
        EXCLUDE_SINGLEUSER="EXCLUDE_SINGLEUSER"
        shift
        ;;

    --no-tip)
        EXCLUDE_TIP="EXCLUDE_TIP"
        shift
        ;;
        
    --no-weave)
        EXCLUDE_WEAVE="EXCLUDE_WEAVE"
        shift
        ;;

    --no-pytest)
        EXCLUDE_PYTEST="EXCLUDE_PYTEST"
        shift
        ;;
        
    --no-ops-pytest)
        EXCLUDE_OPS_PYTEST="EXCLUDE_OPS_PYTEST"
        shift
        ;;

    --no-starter-notebooks)
        EXCLUDE_STARTER_NOTEBOOKS="EXCLUDE_STARTER_NOTEBOOKS"
        shift
        ;;

    --no-test-notebooks)
        EXCLUDE_TEST_NOTEBOOKS="EXCLUDE_TEST_NOTEBOOKS"
        shift
        ;;

    --no-demo-notebooks)
        EXCLUDE_DEMO_NOTEBOOKS="EXCLUDE_DEMO_NOTEBOOKS"
        shift
        ;;

    --no-notebooks)
        EXCLUDE_STARTER_NOTEBOOKS="EXCLUDE_STARTER_NOTEBOOKS"
        EXCLUDE_TEST_NOTEBOOKS="EXCLUDE_TEST_NOTEBOOKS"
        EXCLUDE_DEMO_NOTEBOOKS="EXCLUDE_DEMO_NOTEBOOKS"
        shift
        ;;

    -- )
        shift
        break
        ;;

    * )
        echo "unexpected argument $1"
        exit 1
        ;;
esac
done


FAILURES=0
fail() {
    FAILURES=$(( ${FAILURES} + 1 ))
    [[ ! -z "$1" ]] && echo "FAILURE: $@"
}

fix_prerequisites() {
    #here is the minimum criterion for a working opal
    read -r -d '' opal_verification<<'EOF'
import opal
import opal.flow
import opal.publish
import opal.query
EOF

    # make sure opal is installed in each environment
    echo "checking for OPAL python package"
    if [[ -z "${EXCLUDE_SINGLEUSER}" ]] ; then
        if ! ${SINGLEUSER_BIN} -c "${opal_verification}" &> /dev/null ; then
            echo "OPAL packages not found in singleuser environment"
            ${SINGLEUSER_BIN} -m pip install ${OPAL_ROOT}/opal-packages
            if (( $? != 0 )) ; then
                echo "failed to install opal packages"
                exit 1
            fi
        fi
    fi

    # make sure the kernels are accessible
    # this will overwrite ~/local/share/jupyter/kernels/{singleuser,torch}/
    echo
    echo "Setting up conda kernels for automated testing."
    if [[ -z "${EXCLUDE_SINGLEUSER}" ]] ; then
        ${SINGLEUSER_BIN} -m ipykernel install --name ${SINGLEUSER_ENV} --user
    fi
}

tip_tests() {
    echo
    echo "TIP tests"
    tests || fail "TIP tests failed."
}

weave_tests() {
    if [[ -z "${EXCLUDE_SINGLEUSER}" ]] ; then
        echo
        echo "pytest weave (singleuser)"
        ${SINGLEUSER_BIN} -m pytest --pyargs weave -vv \
            || fail "pytest (singleuser)"
    fi
}

pytest_tests() {
    if [[ -z "${EXCLUDE_SINGLEUSER}" ]] ; then
        echo
        echo "pytest tests (singleuser)"
        ${SINGLEUSER_BIN} -m pytest -vv ${OPAL_ROOT}/opal-packages \
            || fail "pytest (singleuser)"
    fi
}

pytest_ops_tests() {
    if [[ -z "${EXCLUDE_SINGLEUSER}" ]] ; then
        echo
        echo "pytest ops tests (singleuser)"
        ${SINGLEUSER_BIN} -m pytest -vv ${OPAL_ROOT}/devops-software/ops-tests \
            || fail "pytest ops test (singleuser)"
    fi
}

notebook_tests() {
    local name="notebook_tests"
    local conda_env="${SINGLEUSER_ENV}"
    local args=$(getopt -o "c:n:" \
        --long conda_environment:,name: \
        -n notebook_tests \
        -- "$@" )
    eval set -- "$args"
    while true ; do
        case "$1" in
        -c|--conda_environment)
            conda_env="$2"
            shift 2
            ;;
        -n|--name)
            name="$2"
            shift 2
            ;;
        --)
            shift
            break
            ;;
        esac
    done

    local count="${#@}"
    if (( count == 0 )) ; then
        echo "no notebooks specified"
        return
    fi

    local notebooks=($@)
    local failures=0
    local skipped=0
    local output_dir=$(mktemp -d "${TMPDIR:-/tmp/}/notebook_tests.XXXXXXX")
    for i in ${!notebooks[@]} ; do
        local f=${notebooks[$i]}
        local nb=$(basename $f)
        local out="${output_dir}/${nb}.out"

        stdbuf -o0 echo -n "[$((i + 1))/${count}] ${nb} ... "

        # If the notebook name contains the string "Dask" ...
        if echo "$nb" | grep -q "Dask"; then
           
            # Check if dask-gateway is installed
            if ! pip show dask-gateway &> /dev/null; then
                echo "SKIPPING (dask-gateway is not installed)"
                skipped=$(( ${skipped} + 1 ))
                continue
            fi
        fi
     
        # If the notebook name contains the string "gpu" ...
        if echo "$nb" | grep -q "gpu"; then
        
            # Check if the nvidia-smi command exists and is executable.
            if ! command -v nvidia-smi &> /dev/null; then
                echo "SKIPPING (No Nvidia GPU Avaialble)"
                skipped=$(( ${skipped} + 1 ))
                continue
             fi
         fi
        
        TEST="${TEST}" TEST_ENV="${TEST_ENV}" \
        jupyter nbconvert --execute "$f" \
            --to notebook \
            --ExecutePreprocessor.kernel_name="${conda_env}" \
            --inplace &> ${out}
        if (( $? == 0 )) ; then
            echo "SUCCESS"
        else
            echo "FAILURE"
            if [[ ! -z "${VERBOSE}" ]] ; then
                echo
                echo "${f}"
                cat ${out}
                echo
            fi

            failures=$(( ${failures} + 1 ))
        fi
    done

    if (( ${failures} != 0 )) ; then
        fail "${failures} notebook(s) in ${name} test failed"
    fi

    if (( ${skipped} != 0 )) ; then
        echo "${skipped} notebook(s) in ${name} test was/were skipped"
    fi
}

#starter notebooks should be run in all environments always
starter_notebook_tests() {
    nb_root=${1:-${OPAL_ROOT}/starter-notebooks}
    local notebooks=( $(find "${nb_root}"  -name "*.ipynb" \
        | grep -v "ipynb_checkpoints" ) )

    if [[ -z "${EXCLUDE_SINGLEUSER}" ]] ; then
        echo
        echo "running starter notebooks (singleuser)"
        notebook_tests --conda_environment ${SINGLEUSER_ENV} \
            --name "starter_notebook_tests (singleuser)" \
            ${notebooks[@]}
    fi
}

#these might have tests that can only be run in one environment
#those tests should have the environment (singleuser, torch) in
#the name
test_notebook_tests() {
    nb_root=${1:-${OPAL_ROOT}/test-notebooks}
    local common_notebooks=( $(find "${nb_root}"  -name "*.ipynb" \
        | grep -v "ipynb_checkpoints" \
        | grep -iv "singleuser" \
        | grep -iv "torch" ) )
    local singleuser_notebooks=( $(find "${nb_root}"  -name "*.ipynb" \
        | grep -v "ipynb_checkpoints" \
        | grep -i "singleuser" ) )
    local torch_notebooks=( $(find "${nb_root}"  -name "*.ipynb" \
        | grep -v "ipynb_checkpoints" \
        | grep -i "torch" ) )

    if [[ -z "${EXCLUDE_SINGLEUSER}" ]] ; then
        echo
        echo "running test notebooks (singleuser)"
        singleuser_notebooks+=( "${common_notebooks[@]}" )
        notebook_tests --conda_environment ${SINGLEUSER_ENV} \
            --name "test_notebook_tests (singleuser)" \
            "${singleuser_notebooks[@]}"
    fi
}

demo_notebook_tests() {
    nb_root=${1:-${OPAL_ROOT}/demo-notebooks}
    local common_notebooks=( $(find "${nb_root}"  -name "*.ipynb" \
        | grep -v "ipynb_checkpoints" \
        | grep -iv "singleuser" \
        | grep -iv "torch" ) )
    local singleuser_notebooks=( $(find "${nb_root}"  -name "*.ipynb" \
        | grep -v "ipynb_checkpoints" \
        | grep -i "singleuser" ) )
    local torch_notebooks=( $(find "${nb_root}"  -name "*.ipynb" \
        | grep -v "ipynb_checkpoints" \
        | grep -i "torch" ) )

    if [[ -z "${EXCLUDE_SINGLEUSER}" ]] ; then
        echo
        echo "running demo notebooks (singleuser)"
        singleuser_notebooks+=( "${common_notebooks[@]}" )
        notebook_tests --conda_environment ${SINGLEUSER_ENV} \
            --name "demo_notebook_tests (singleuser)" \
            "${singleuser_notebooks[@]}"
    fi
}

test_default_environment() {
    local default_env

    stdbuf -o0 echo -n "default environment test ... "
    default_env=$(bash -l -c "echo \$CONDA_DEFAULT_ENV")
    if [[ "${default_env}" == "singleuser" ]] ; then
        echo "SUCCESS"
    else
        echo "FAILURE"
        fail "Default env \"$CONDA_DEFAULT_ENV\" is not singleuser"
    fi
}

main() {
    fix_prerequisites
    [[ -z "${EXCLUDE_TIP}" ]] && tip_tests
    [[ -z "${EXCLUDE_WEAVE}" ]] && weave_tests
    [[ -z "${EXCLUDE_PYTEST}" ]] && pytest_tests
    [[ -z "${EXCLUDE_OPS_PYTEST}" ]] && pytest_ops_tests
    [[ -z "${EXCLUDE_STARTER_NOTEBOOKS}" ]] && \
        starter_notebook_tests ${OPAL_ROOT}/starter-notebooks
    [[ -z "${EXCLUDE_TEST_NOTEBOOKS}" ]] && \
        test_notebook_tests ${OPAL_ROOT}/test-notebooks
    [[ -z "${EXCLUDE_DEMO_NOTEBOOKS}" ]] && \
        demo_notebook_tests ${OPAL_ROOT}/demo-notebooks

    test_default_environment

    if (( ${FAILURES} != 0 )) ; then
        echo "FAILURE"
        exit 1
    fi
}

main

