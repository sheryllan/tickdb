#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

VENV_NAME=bar_checks
echo "Setting up virtual environment ${VENV_NAME}"

if ! conda info --envs | grep -q ${VENV_NAME}; then
    echo "Creating virtual environment"
    conda create -n --yes ${VENV_NAME} python
    echo "Virtual environment created"
fi

source activate ${VENV_NAME}
echo "Virtual environment activated"

REQUIREMNTS=${DIR}/requirements.txt
echo "Installing packages from ${REQUIREMNTS}"
pip install -r ${REQUIREMNTS}

echo "Packages installed:"
pip list

source deactivate
