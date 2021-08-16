//
// Created by jennifer on 8/11/21.
//

#ifndef SMDBRPC_TRIGGERDEMOTIONCALLDATA_H
#define SMDBRPC_TRIGGERDEMOTIONCALLDATA_H

#include "smdbrpc.grpc.pb.h"
using smdbrpc::TriggerDemotionRequest;
using smdbrpc::TriggerDemotionReply;
using smdbrpc::TriggerDemotionStatus;

class TriggerDemotionCallData final : public Call {
public:
    TriggerDemotionCallData(HotshardGateway::AsyncService* service, ServerCompletionQueue* cq)
    : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
      Proceed();
    }

    void Proceed() override {

      if (status_ == CREATE) {
        status_ = PROCESS;
        service_ ->RequestTriggerDemotion(&ctx_, &request_, &responder_,
                                          cq_, cq_, this);
      } else if (status_ == PROCESS) {

        for (uint64_t key : request_.keys()) {
          std::cout << "Trigger demotion for key " << key << std::endl;

          TriggerDemotionStatus* triggerDemotionStatus = reply_.add_triggerdemotionstatuses();
          triggerDemotionStatus->set_key(key);
          triggerDemotionStatus->set_isdemotiontriggered(true);
        }

        status_ = FINISH;
        responder_.Finish(reply_, Status::OK, this);

      } else {
        new TriggerDemotionCallData(service_, cq_);
        delete this;
      }
    }

private:
    HotshardGateway::AsyncService* service_;
    ServerCompletionQueue* cq_;
    ServerContext ctx_;

    TriggerDemotionRequest request_;
    TriggerDemotionReply reply_;

    ServerAsyncResponseWriter<TriggerDemotionReply> responder_;

    enum CallStatus {CREATE, PROCESS, FINISH};
    CallStatus status_;
};


#endif //SMDBRPC_TRIGGERDEMOTIONCALLDATA_H
