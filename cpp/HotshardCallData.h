//
// Created by jennifer on 8/11/21.
//

#ifndef SMDBRPC_HOTSHARDCALLDATA_H
#define SMDBRPC_HOTSHARDCALLDATA_H

#include "Call.h"

#include "smdbrpc.grpc.pb.h"
#include "CallData.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using smdbrpc::HotshardRequest;
using smdbrpc::HotshardReply;
using smdbrpc::HotshardGateway;
using smdbrpc::HLCTimestamp;

class HotshardCallData final : public Call {
public:
    HotshardCallData(HotshardGateway::AsyncService* service, ServerCompletionQueue* cq)
    : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
      Proceed();
    }

    void Proceed() override {
      if (status_ == CREATE) {
        status_ = PROCESS;
        service_->RequestContactHotshard(&ctx_, &request_, &responder_,
                                         cq_, cq_, this);
      } else if (status_ == PROCESS) {
        std::cout << "PROCESS" << std::endl;
        reply_.set_is_committed(true);
        std::cout << "reply committed" << std::endl;

        for (uint64_t key : request_.read_keyset()) {
          smdbrpc::KVPair *kvPair = reply_.add_read_valueset();
          kvPair->set_key(key);
          uint64_t val = 1994214;
          kvPair->set_value(val);


          std::cout << "key, val ("
          << key << ", " << val
          << ")" << std::endl;
        }

        status_ = FINISH;
        responder_.Finish(reply_, Status::OK, this);
      } else {
        new HotshardCallData(service_, cq_);
        delete this;
      }
    }

private:
    HotshardGateway::AsyncService* service_;
    ServerCompletionQueue* cq_;
    ServerContext ctx_;

    HotshardRequest request_;
    HotshardReply reply_;

    ServerAsyncResponseWriter<HotshardReply> responder_;

    enum CallStatus {CREATE, PROCESS, FINISH};
    CallStatus status_;
};

#endif //SMDBRPC_HOTSHARDCALLDATA_H
