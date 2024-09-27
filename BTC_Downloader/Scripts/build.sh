#!/bin/bash
set -e

clear
source utls/_pre_build.sh

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