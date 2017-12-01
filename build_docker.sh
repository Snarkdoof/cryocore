#!/bin/sh

target=$1

if [ "$target" = "cleanup" ]; then
  echo "Removing dangling docker images"
  # docker rmi $(docker images --filter "dangling=true" -q --no-trunc)
  exit
fi

if ! test -e docker/$target ; then
  echo "Missing docker target $target"
  echo "Valid options could be:"
  dir docker/
  exit
fi

# Do we have rights?
docker stats --no-stream >/dev/null 2>/dev/null
if test $? != 0; then
  echo "Can't run docker - is it installed and do you have rights? Try running with sudo"
  exit
fi

echo "Building docker", $target

if test -L Dockerfile; then 
  rm Dockerfile
fi
ln -s docker/$target/Dockerfile .


docker build -t $target . 
rm Dockerfile
#if test $2==test; then
#  sudo docker run $target
#fi


# echo "Pushing it to localhost:5000"
# sudo docker tag $target localhost:5000/$target:latest
# sudo docker push localhost:5000/$target:latest

