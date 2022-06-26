#!/bin/bash

cd ~/smdbrpc/protos || exit
protoc --go_out=../go/build/gen --go-grpc_out=../go/build/gen *.proto

cd ~/smdbrpc/protos || exit
python3 -m grpc_tools.protoc -I. --python_out=../python --grpc_python_out=../python *.proto

cd ~/smdbrpc/cpp || exit
rm -rf cmake/build
mkdir -p cmake/build
pushd cmake/build || exit
cmake -DCMAKE_PREFIX_PATH=$MY_INSTALL_PATH ../..
make -j


