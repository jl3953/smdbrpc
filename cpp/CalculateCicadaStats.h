//
// Created by jennifer on 8/22/21.
//

#ifndef SMDBRPC_CALCULATECICADASTATS_H
#define SMDBRPC_CALCULATECICADASTATS_H

#include "Call.h"

#include "smdbrpc.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using smdbrpc::HotshardGateway;
using smdbrpc::HLCTimestamp;

class CalculcateCicadaStats final : public Call {

public:
  CalculcateCicadaStats(HotshardGateway::AsyncService *service,
                        ServerCompletionQueue *cq)
    : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
    Proceed();
  }
  
  void Proceed() override {
    if (status_ == CREATE) {
      status_ = PROCESS;
      service_->RequestCalculateCicadaStats(&ctx_, &request_, &responder_,
                                            cq_, cq_, this);
    } else if (status_ == PROCESS) {
      reply_.set_demotion_only(false);
      reply_.set_qps_avail_for_promotion(100);
      reply_.set_num_keys_avail_for_promotion(100);
      reply_.set_qps_at_nth_percentile(0);
      
      std::cout << "cpu_target: " << request_.cpu_target()
                << ", cpu_ceiling: " << request_.cpu_ceiling()
                << ", cpu_floor: " << request_.cpu_floor()
                << ", mem_target: " << request_.mem_target()
                << ", mem_ceiling: "<< request_.mem_ceiling()
                << ", mem_floor: " << request_.mem_floor()
                << ", percentile_n: " << request_.percentile_n()
                << std::endl;
      
      status_ = FINISH;
      responder_.Finish(reply_, Status::OK, this);
    } else {
      new HotshardCallData(service_, cq_);
      delete this;
    }
  }

private:
  HotshardGateway::AsyncService *service_;
  ServerCompletionQueue *cq_;
  ServerContext ctx_;
  
  smdbrpc::CalculateCicadaReq request_;
  smdbrpc::CalculateCicadaStatsResp reply_;
  
  ServerAsyncResponseWriter<smdbrpc::CalculateCicadaStatsResp> responder_;
  
  enum CallStatus {
    CREATE, PROCESS, FINISH
  };
  CallStatus status_;
  
};

#endif //SMDBRPC_CALCULATECICADASTATS_H
