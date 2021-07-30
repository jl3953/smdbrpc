#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "trigger_demotion_server.h"


int main(int argc, char** argv) {

  // concurrency
  int concurrency = 1;
  if (argc > 1) {
    concurrency = atol(argv[1]);
  }

  char *temp;
  std::cout << concurrency << std::endl;
  if (argc >= 2)
    concurrency = static_cast<int>(strtol(argv[1], &temp, 10));

  std::string port = "50053";
  if (argc >= 3)
    port = argv[2];

  TriggerDemotionServerImpl server;
  std::thread triggerDemotionServer(&TriggerDemotionServerImpl::Run,
                                    &server,
                                    port, concurrency);
  triggerDemotionServer.join();

  return 0;
}
