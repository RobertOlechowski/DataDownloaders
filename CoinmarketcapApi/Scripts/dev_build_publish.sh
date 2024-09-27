#!/bin/bash
set -e

clear

# docker login ghcr.io -u robertolechowski
# github login

github_repo="ghcr.io/robertolechowski"
image_name="coinmarketcap-downloader"


version=$(python3 buildUtls.py get_ver)
build_time=$(python3 buildUtls.py time)

echo
echo
echo 'Build and docker image and increase version'
echo Image        : $image_name
echo Version      : $version
echo Tag          : dev
echo Build time   : $build_time
echo
echo

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