//
// Created by jennifer on 7/20/21.
//

#ifndef SMDBRPC_TRIGGER_DEMOTION_SERVER_H
#define SMDBRPC_TRIGGER_DEMOTION_SERVER_H


#include <grpcpp/grpcpp.h>
#include <thread>
#include "triggerdemotion.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncReader;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::Status;
using smdbrpc::TriggerDemotionGateway;
using smdbrpc::TriggerDemotionRequest;
using smdbrpc::TriggerDemotionReply;
using smdbrpc::TriggerDemotionStatus;

class TriggerDemotionServerImpl final {
public:
    TriggerDemotionServerImpl() = default;

    ~TriggerDemotionServerImpl() {
      server_->Shutdown();

      for (auto& cq: cq_vec_)
        cq->Shutdown();
    }

    TriggerDemotionServerImpl(TriggerDemotionServerImpl &&) = default;

    void Run(const std::string& port, int concurrency) {
      std::string server_address("0.0.0.0:" + port);

      ServerBuilder builder;
      builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
      builder.RegisterService(&service_);

      for (int i = 0; i < concurrency; i++) {
        cq_vec_.emplace_back(builder.AddCompletionQueue().release());
      }

      server_ = builder.BuildAndStart().release();
      std::cout << "Server listening on " << server_address << std::endl;

      for (int i = 0; i < concurrency; i++) {
        server_threads_.emplace_back(std::thread([this, i] { HandleRpcs(i);}));
      }

      for (auto& thread: server_threads_)
        thread.join();
    }

private:

    class CallData {
    public:
        CallData(TriggerDemotionGateway::AsyncService* service, ServerCompletionQueue* cq)
        : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
          Proceed();
        }

        void Proceed() {
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
            new CallData(service_, cq_);
            delete this;
          }
        }


    private:
        TriggerDemotionGateway::AsyncService* service_;
        ServerCompletionQueue* cq_;
        ServerContext ctx_;

        TriggerDemotionRequest request_;
        TriggerDemotionReply reply_;

        ServerAsyncResponseWriter<TriggerDemotionReply> responder_;

        enum CallStatus {CREATE, PROCESS, FINISH};
        CallStatus status_;


    };

    [[noreturn]] void HandleRpcs(int i) {
      new CallData(&service_, cq_vec_[i]);
      void *tag;
      bool ok;
      while (true) {
        cq_vec_[i]->Next(&tag, &ok);
        static_cast<CallData *>(tag)->Proceed();
      }
    }

    std::vector<ServerCompletionQueue*> cq_vec_;
    TriggerDemotionGateway::AsyncService service_;
    Server* server_{};
    std::list<std::thread> server_threads_;
};

#endif //SMDBRPC_TRIGGER_DEMOTION_SERVER_H
