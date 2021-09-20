//
// Created by jennifer on 9/3/21.
//

#ifndef SMDBRPC_PROMOTEKEYS_H
#define SMDBRPC_PROMOTEKEYS_H

#include "smdbrpc.grpc.pb.h"
#include "smdbrpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using smdbrpc::HotshardGateway;
using smdbrpc::HLCTimestamp;

class PromoteKeys final : public Call {
public:
  PromoteKeys(HotshardGateway::AsyncService *service,
              ServerCompletionQueue *cq)
              : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
    Proceed();
  }
  
  void Proceed() override {
    if (status_ == CREATE) {
      status_ = PROCESS;
      service_->RequestPromoteKeys(&ctx_, &request_, &responder_,
                                            cq_, cq_, this);
    } else if (status_ == PROCESS) {
      smdbrpc::KeyMigrationStatus* status = reply_
        .add_were_successfully_migrated();
      status->set_is_successfully_migrated(true);
      
      for (const auto& key : request_.keys()) {
        std::cout << "key: " << key.key()
        << ", value: " << key.value()
        << ", timestamp: " << key.timestamp().walltime()
        << ", " << key.timestamp().logicaltime()
        << std::endl;
      }
      
      status_ = FINISH;
      responder_.Finish(reply_, Status::OK, this);
    } else {
      new PromoteKeys(service_, cq_);
      delete this;
    }
  }
  
private:
  HotshardGateway::AsyncService *service_;
  ServerCompletionQueue *cq_;
  ServerContext ctx_;
  
  smdbrpc::PromoteKeysReq request_;
  smdbrpc::PromoteKeysResp reply_;
  
  ServerAsyncResponseWriter<smdbrpc::PromoteKeysResp> responder_;
  
  enum CallStatus {
    CREATE, PROCESS, FINISH
  };
  CallStatus status_;
  
};

#endif //SMDBRPC_PROMOTEKEYS_H
