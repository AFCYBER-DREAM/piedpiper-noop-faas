#!/bin/bash

tox -e build-docker
version=$(python -c 'from setuptools_scm import get_version; print(get_version())' | tr '+' '-')
echo "Pushing version ${version}"
docker login -u="${DOCKER_USERNAME}" -p="${DOCKER_PASSWORD}" quay.io
docker push quay.io/afcyber-dream/piedpiper-flake8-faas:"${version}"
docker push quay.io/afcyber-dream/piedpiper-flake8-faas:latest
