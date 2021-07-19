#include <iostream>

#include <grpcpp/grpcpp.h>
#include <thread>
#include "demotehotkeys.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::Status;
using smdbrpc::DemoteHotkeysGateway;
using smdbrpc::KVVersion;
using smdbrpc::KVDemotionStatus;
using smdbrpc::HLCTimestamp;

KVVersion MakeKVVersion(const std::string& key,
                        const std::string& value,
                        int64_t walltime,
                        int32_t logicaltime,
                        int64_t hotness) {
  KVVersion kvVersion;
  kvVersion.set_key(key);
  kvVersion.set_value(value);

  auto* timestamp = new HLCTimestamp();
  timestamp->set_walltime(walltime);
  timestamp->set_logicaltime(logicaltime);
  kvVersion.set_allocated_timestamp(timestamp);

  kvVersion.set_hotness(hotness);

  return kvVersion;

}

class DemoteHotkeysGatewayClient {
public:
  explicit DemoteHotkeysGatewayClient(const std::shared_ptr<Channel>& channel) : stub_(DemoteHotkeysGateway::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  void DemoteHotkeys() {

    ClientContext context;

    std::shared_ptr<ClientReaderWriter<KVVersion, KVDemotionStatus> > stream(
        stub_->DemoteHotkeys(&context));

    std::cout << "std::shared_ptr stream" << std::endl;

    std::thread writer([stream]() {
      std::vector<KVVersion> kvVersions{
          MakeKVVersion("1994214", "jennifer", 1994214, 1, 214),
          MakeKVVersion("1994812", "jeff", 1994812, 2, 812)
      };

        std::cout << "make vector" << std::endl;
        for (const KVVersion& kvVersion : kvVersions) {
          std::cout << "Sending message " << kvVersion.key()
                    << ", " << kvVersion.value()
                    << " at " << kvVersion.timestamp().walltime()
                    << std::endl;
          stream->Write(kvVersion);
        }
        stream->WritesDone();
    });


    std::cout << "make thread function" << std::endl;

    KVDemotionStatus demotionStatus;
    while (stream->Read(&demotionStatus)) {
      std::cout << "Replied for key " << demotionStatus.key()
                << " with return code " << demotionStatus.is_successfully_demoted()
                << std::endl;
    }
    writer.join();

    Status status = stream->Finish();
    if (!status.ok()) {
      std::cout << "DemoteHotkeys rpc failed" << std::endl;
    }
  }

private:
    std::unique_ptr<DemoteHotkeysGateway::Stub> stub_;

};

int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection ot an endpoint specified by
  // the argument "--target=" which is the only expected argument.
  // We indicate that the channel isn't authenticated (use of
  // InsecureChannelCredentials()).

  std::string target_str;
  std::string arg_str("--target");
  if (argc > 1) {
    std::string arg_val = argv[1];
    size_t start_pos = arg_val.find(arg_str);
    if (start_pos != std::string::npos) {
      start_pos += arg_str.size();
      if (arg_val[start_pos] == '=') {
        target_str = arg_val.substr(start_pos + 1);
      } else {
        std::cout << "The only correct argument syntax is --target="
                  << std::endl;
        return 0;
      }
    } else {
      std::cout << "The only acceptable argument is --target=" << std::endl;
      return 0;
    }
  } else {
    target_str = "localhost:50052";
  }
  DemoteHotkeysGatewayClient client(grpc::CreateChannel(
      target_str, grpc::InsecureChannelCredentials()));
  client.DemoteHotkeys();

  return 0;
}