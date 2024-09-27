#!/bin/bash
set -e

clear

# docker login ghcr.io -u robertolechowski
# github login

github_repo="ghcr.io/robertolechowski"
image_name="eth-to-db"


version=$(python3 buildUtls.py get_ver)
build_time=$(python3 buildUtls.py time)

echo
echo
echo 'Build and docker image and increase version'
echo Image        : $image_name
echo Version      : $version
echo Tag          : latest
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
echo '=== PUSH to GitHub ==='
echo
docker tag $image_name:$version $github_repo/$image_name:latest
docker tag $image_name:$version $github_repo/$image_name:$version

docker push $github_repo/$image_name:latest
#docker push $github_repo/$image_name:$version

echo
echo '=== Increment version number ==='
echo
version=$(python3 buildUtls.py inc_ver)

echo
echo '=== DONE ==='
echo