/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>

#ifdef BAZEL_BUILD
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "smdbrpc.grpc.pb.h"
#endif

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

//// Logic and data behind the server's behavior.
//class HotshardGatewayServiceImpl final : public HotshardGateway::Service {
//  Status ContactHotshard(ServerContext* context, const HotshardRequest* request,
//                  HotshardReply* reply) override {
//
//      //std::cout << "hlc.walltime:[" << request->hlctimestamp().walltime()
//      //  << "], logical:[" << request->hlctimestamp().logicaltime()
//      //  << "]" << std::endl;
//
//      //std::cout << "writes:[";
//      //for (const smdbrpc::KVPair& kvPair : request->write_keyset()) {
//      //    std::cout << "(" << kvPair.key()
//      //      << ", " << kvPair.value()
//      //      << "), ";
//      //}
//      //std::cout << "]" << std::endl;
//
//      //std::cout << "reads:[";
//      //for (uint64_t key : request->read_keyset()) {
//      //    std::cout << key << ", ";
//      //}
//      //std::cout << "]\n"
//      //  << "===================" << std::endl;
//
//    reply->set_is_committed(true);
//
////    for (int i = 0; i < 245000; i++) {
////        smdbrpc::KVPair* kvPair = reply->add_read_valueset();
////        kvPair->set_key(std::to_string(i));
////        kvPair->set_value(std::to_string(i));
////    }
//
//
//    smdbrpc::KVPair* jennBday = reply->add_read_valueset();
//    jennBday->set_key(1994214);
//    jennBday->set_value(1994214);
//
//      smdbrpc::KVPair* halloween = reply->add_read_valueset();
//      halloween->set_key(20201031);
//      halloween->set_value(20201031);
//
//    return Status::OK;
//  }
//};
//
//void RunServer() {
//  std::string server_address("0.0.0.0:50051");
//  HotshardGatewayServiceImpl service;
//
//  grpc::EnableDefaultHealthCheckService(true);
//  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
//  ServerBuilder builder;
//  // Listen on the given address without any authentication mechanism.
//  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
//  // Register "service" as the instance through which we'll communicate with
//  // clients. In this case it corresponds to an *synchronous* service.
//  builder.RegisterService(&service);
//  // Finally assemble the server.
//  std::unique_ptr<Server> server(builder.BuildAndStart());
//  std::cout << "Server listening on " << server_address << std::endl;
//
//  // Wait for the server to shutdown. Note that some other thread must be
//  // responsible for shutting down the server for this call to ever return.
//  server->Wait();
//}

class ServerImpl final {
public:
    ~ServerImpl() {
        server_->Shutdown();

        cq_->Shutdown();

    }

    void Run(const std::string& port) {
        std::string server_address("0.0.0.0:" + port);

        ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service_);

        //auto parallelism = std::max(1u, std::thread::hardware_concurrency());
        cq_ = builder.AddCompletionQueue().release();

        server_ = builder.BuildAndStart().release();
        std::cout << "Server listening on " << server_address << std::endl;

        HandleRpcs();
    }



private:

    class CallData {
    public:
        CallData(HotshardGateway::AsyncService* service, ServerCompletionQueue* cq)
        : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
            Proceed();
        }

        void Proceed() {
            if (status_ == CREATE) {
                status_ = PROCESS;

                service_->RequestContactHotshard(&ctx_, &request_, &responder_,
                                                 cq_, cq_, this);
            } else if (status_ == PROCESS) {
                new CallData(service_, cq_);

                reply_.set_is_committed(true);

                status_ = FINISH;
                responder_.Finish(reply_, Status::OK, this);

            } else {
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

    [[noreturn]] void HandleRpcs() {
        new CallData(&service_, cq_);
        void *tag;
        bool ok;
        while (true) {
            cq_->Next(&tag, &ok);
            static_cast<CallData *>(tag)->Proceed();
        }

    }

    ServerCompletionQueue* cq_;
    HotshardGateway::AsyncService service_;
    Server* server_;
};

int main(int argc, char** argv) {

    int parallelism = atoi(argv[1]);
    std::list<std::thread> pool;

    for (int i = 0; i < parallelism; i++) {
        int port = 50051 + i;

        pool.emplace_back(std::thread(&ServerImpl::Run,
                                      new ServerImpl(),
                                      std::to_string(port)));
    }

    for (auto& thread : pool)
        thread.join();

  return 0;
}
