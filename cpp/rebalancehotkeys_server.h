//
// Created by jennifer on 7/30/21.
//

#ifndef SMDBRPC_REBALANCEHOTKEYS_SERVER_H
#define SMDBRPC_REBALANCEHOTKEYS_SERVER_H

#include <grpcpp/grpcpp.h>
#include <thread>
#include "rebalancehotkeys.grpc.pb.h"
#include "Call.h"

using grpc::Server;
using grpc::ServerAsyncReader;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::Status;
using smdbrpc::RebalanceHotkeysGateway;
using smdbrpc::KeyStatsRequest;
using smdbrpc::CicadaStatsResponse;
using smdbrpc::KeyStat;

class RebalanceHotkeysCallData final : public Call{

public:
    RebalanceHotkeysCallData(RebalanceHotkeysGateway::AsyncService
    * service,
    ServerCompletionQueue *cq
    )
    :
    service_(service), cq_(cq), responder_( &
    ctx_),
    status_(CREATE) {
        Proceed();
    }

    void Proceed() override {
      if (status_ == CREATE) {
        status_ = PROCESS;
        service_->RequestRequestCicadaStats(&ctx_, &request_, &responder_,
                                            cq_, cq_, this);
      } else if (status_ == PROCESS) {

        KeyStat *jennKeyStat = reply_.add_keystats();
        jennKeyStat->set_key(214);
        jennKeyStat->set_qps(214);
        jennKeyStat->set_writeqps(214);

        KeyStat *jeffKeyStat = reply_.add_keystats();
        jeffKeyStat->set_key(812);
        jeffKeyStat->set_qps(812);
        jeffKeyStat->set_writeqps(812);

        reply_.set_cpuusage(80);
        reply_.set_memusage(90);

        status_ = FINISH;
        responder_.Finish(reply_, Status::OK, this);

      } else {
        new RebalanceHotkeysCallData(service_, cq_);
        delete this;
      }
    }


private:
    RebalanceHotkeysGateway::AsyncService *service_;
    ServerCompletionQueue *cq_;
    ServerContext ctx_;

    KeyStatsRequest request_;
    CicadaStatsResponse reply_;

    ServerAsyncResponseWriter<CicadaStatsResponse> responder_;

    enum CallStatus {
        CREATE, PROCESS, FINISH
    };
    CallStatus status_;

};

#endif //SMDBRPC_REBALANCEHOTKEYS_SERVER_H
