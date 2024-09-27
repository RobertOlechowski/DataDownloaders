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
echo '=== PUSH to GitHub ==='
echo
docker tag $image_name:$version $github_repo/$image_name:latest
docker tag $image_name:$version $github_repo/$image_name:$version

docker push $github_repo/$image_name:latest
#docker push $github_repo/$image_name:$version

echo
echo '=== Increment version number ==='
echo
version=$(python3 utls/buildUtls.py inc_ver)

echo
echo '=== DONE ==='
echo