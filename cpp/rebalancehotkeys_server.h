//
// Created by jennifer on 7/30/21.
//

#ifndef SMDBRPC_REBALANCEHOTKEYS_SERVER_H
#define SMDBRPC_REBALANCEHOTKEYS_SERVER_H

#include <grpcpp/grpcpp.h>
#include <thread>
#include "rebalancehotkeys.grpc.pb.h"

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

class RebalanceHotkeysServerImpl final {
public:
    ~RebalanceHotkeysServerImpl() {
      server_->Shutdown();

      for (auto& cq: cq_vec_)
        cq->Shutdown();
    }

    void Run(const std::string& port, int concurrency) {
      std::string server_address("0.0.0.0:50054");

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
        CallData(RebalanceHotkeysGateway::AsyncService* service, ServerCompletionQueue* cq)
        : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
          Proceed();
        }

        void Proceed() {
          if (status_ == CREATE) {
            status_ = PROCESS;
            service_ ->RequestRequestCicadaStats(&ctx_, &request_, &responder_,
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
            new CallData(service_, cq_);
            delete this;
          }
        }


    private:
        RebalanceHotkeysGateway::AsyncService* service_;
        ServerCompletionQueue* cq_;
        ServerContext ctx_;

        KeyStatsRequest request_;
        CicadaStatsResponse reply_;

        ServerAsyncResponseWriter<CicadaStatsResponse> responder_;

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
    RebalanceHotkeysGateway::AsyncService service_;
    Server* server_{};
    std::list<std::thread> server_threads_;

};

#endif //SMDBRPC_REBALANCEHOTKEYS_SERVER_H
