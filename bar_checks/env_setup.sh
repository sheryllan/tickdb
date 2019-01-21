#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

VENV_PATH=venv
echo "Setting up virtual environment in directory ${DIR}/${VENV_PATH}"

if ! [ -d ${VENV_PATH} ]; then
    echo "Creating virtual environment"
    mkdir ${VENV_PATH}

    python virtualenv ${VENV_PATH}
    echo "Virtual environment created"
fi

VENV_BIN=${VENV_PATH}/bin
source ${VENV_BIN}/activate
echo "Virtual environment activated"

REQUIREMNTS=requirements.txt
echo "Installing packages from ${DIR}/${REQUIREMNTS}"
pip install -r ${REQUIREMNTS}

echo "Packages installed:"
pip freeze

deactive
