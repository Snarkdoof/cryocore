#!/bin/bash

CCDIR=$PWD

# Install dependencies
sudo apt install cmake 

cmake >/dev/null 2>/dev/null
if [ $? != 0 ]; then
    echo "Missing CMake, installing"
    sudo apt install cmake
fi

cd CCshm

if [ ! -x build ]; then
    echo "Build doesnt exist"
    mkdir build
fi

cd build

cmake ../

if [ $? != 0 ]; then
    echo "CMake failed"
    exit
fi

make -j 4

if [ $? == 0 ]; then
    echo "Build completed"
    cp CCshm_*.so $CCDIR
fi

cd $CCDIR

