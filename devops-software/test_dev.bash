#!/bin/bash

# needs pytest
echo "\nGetting Pytest..."
pip install pytest > /dev/null

# install other dependencies here
echo "\nGetting other test dependencies..."

# get newest opal repository
rm -rf ~/new_opal
# can get a specific branch
if [[ $# -gt 0 ]]; then
    git clone --branch $1 https://github.com/309thEDDGE/opal.git ~/new_opal
else
    git clone https://github.com/309thEDDGE/opal.git ~/new_opal
fi

if [[ $? -ne 0 ]]; then
    echo "Failed to clone opal"
    exit 1
fi

# install new packages over the old ones
pip install -e ~/new_opal/opal-packages/*

bash test_all.bash
