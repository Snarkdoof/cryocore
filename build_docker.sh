#!/bin/sh

target=$1

if ! test -e docker/$target ; then
  echo "Missing docker target $target"
  echo "Valid options could be:"
  dir docker/
  exit
fi

echo "Building docker", $target

if test -L Dockerfile; then 
  rm Dockerfile
fi
ln -s docker/$target/Dockerfile .


sudo docker build -t $target . 
rm Dockerfile
#if test $2==test; then
#  sudo docker run $target
#fi


echo "Pushing it to localhost:5000"
sudo docker tag $target localhost:5000/$target:latest
sudo docker push localhost:5000/$target:latest
