#include "rebalancehotkeys_server.h"

int main(int argc, char** argv) {

  int concurrency = 1;

  RebalanceHotkeysServerImpl server;
  std::thread rebalanceHotkeysServer(&RebalanceHotkeysServerImpl::Run,
                                     &server,
                                     "50054", concurrency);
  rebalanceHotkeysServer.join();

  return 0;

}