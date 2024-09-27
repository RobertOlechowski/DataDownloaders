#!/bin/bash
set -e

clear

image_name="eth-to-db"

version=$(python3 buildUtls.py get_ver)
build_time=$(python3 buildUtls.py time)

echo
echo
echo 'Build and docker image and increase version'
echo Image        : $image_name
echo Version      : $version
echo Build time   : $build_time
echo
echo

#=======================================

echo
echo '=== BUILD ==='
echo
docker build -f ../docker/Dockerfile --build-arg BUILD_VERSION=${version}  --build-arg BUILD_TIME=${build_time}  -t $image_name:$version ../.
echo
docker tag $image_name:$version $image_name:latest

echo
echo '=== DONE ==='
echo