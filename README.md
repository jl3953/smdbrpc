Make sure this repo is located in $(go env GOPATH)/src.

## Generate go files from protobuf
```
cd smdbrpc/protos
protoc --go_out=../go --go-grpc_out=../go *.protos
```

## Generate cpp files from protobuf
```
cd smdbrpc/cpp
mkdir -p cmake/build
pushd cmake/build
cmake -DCMAKE_PREFIX_PATH=$MY_INSTALL_PATH ../.. # MY_INSTALL_DIR=$HOME/.local
make -j
```
