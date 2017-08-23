#!/bin/bash

set -o errexit
cd "${0%/*}"

rm -rf dist/
virtualenv env
source ./env/bin/activate
pip3 install wheel --upgrade
pip3 install setuptools --upgrade
python3 setup.py sdist
python3 setup.py bdist_wheel
deactivate
rm -rf env/ build/ mtv_dl.egg-info
