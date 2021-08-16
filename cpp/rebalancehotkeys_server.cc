#include "rebalancehotkeys_server.h"

int main(int argc, char** argv) {

  int concurrency = 1;

  RebalanceHotkeysCallData server;
  std::thread rebalanceHotkeysServer(&RebalanceHotkeysCallData::Run,
                                     &server,
                                     "50054", concurrency);
  rebalanceHotkeysServer.join();

  return 0;

}