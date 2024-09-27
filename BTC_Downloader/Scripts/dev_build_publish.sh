#!/bin/bash
set -e

clear
source utls/_pre_build.sh

#=======================================

echo
echo '=== BUILD ==='
echo
docker build -f ../docker/Dockerfile --build-arg BUILD_VERSION=${version}  --build-arg BUILD_TIME=${build_time}  -t $image_name:dev ../.
echo


echo
echo '=== PUSH to GitHub ==='
echo
docker tag $image_name:dev $github_repo/$image_name:dev
docker push $github_repo/$image_name:dev


echo
echo '=== DONE ==='
echo