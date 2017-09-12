#!/bin/sh

target=$1

if ! test -e docker/$target ; then
  echo "Missing docker target $target"
  echo "Valid options could be:"
  dir docker/
  exit
fi

echo "Building docker", $target

rm Dockerfile
ln -s docker/$target/Dockerfile .


sudo docker build -t $target . 
#if test $2==test; then
#  sudo docker run $target
#fi
