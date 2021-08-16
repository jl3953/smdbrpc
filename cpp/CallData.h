//
// Created by jennifer on 8/11/21.
//

#ifndef SMDBRPC_CALLDATA_H
#define SMDBRPC_CALLDATA_H

#include "smdbrpc.grpc.pb.h"

using grpc::ServerCompletionQueue;
using smdbrpc::HotshardGateway;

struct CallData {
    HotshardGateway::AsyncService* service_;
    ServerCompletionQueue* cq_;
};

#endif //SMDBRPC_CALLDATA_H
