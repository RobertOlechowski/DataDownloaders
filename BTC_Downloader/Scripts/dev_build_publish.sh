#!/bin/bash
set -e

clear

export github_repo="ghcr.io/robertolechowski"
export image_name="btc-downloader"

version=$(python3 utls/buildUtls.py get_ver)
build_time=$(python3 utls/buildUtls.py time)

echo
echo
echo 'Build and docker image and increase version'
echo Image        : $image_name
echo Version      : $version
echo Tag          : latest
echo Build time   : $build_time
echo Repo         : $github_repo
echo
echo

#=======================================

echo
echo '=== BUILD ==='
echo
pip freeze > ../requirements.txt
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