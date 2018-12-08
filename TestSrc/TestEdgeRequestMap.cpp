#define BOOST_TEST_MAIN TestEdgeRequestList

#define DEBUG
#define METRICS

#include <boost/test/unit_test.hpp>
#include "EdgeRequestMap.hpp"
#include "NetflowGenerators.hpp"
#include <thread>

using namespace sam;

zmq::context_t context(1);


typedef EdgeRequestMap<Netflow, SourceIp, DestIp, TimeSeconds,
  LastOctetHashFunction, LastOctetHashFunction,
  StringEqualityFunction, StringEqualityFunction> MapType;

typedef MapType::EdgeRequestType EdgeRequestType;

BOOST_AUTO_TEST_CASE( test_edge_request_map )
{
  size_t numNodes = 2;
  size_t nodeId0 = 0;
  size_t nodeId1 = 1;
  std::vector<std::string> hostnames;
  hostnames.push_back("localhost");
  hostnames.push_back("localhost");
  size_t startingPort = 10000;
  uint32_t hwm = 1000;
  size_t tableCapacity = 1000;
  int timeout = -1;
  size_t numPushSockets = 1;
  size_t numPullThreads = 1;

	// PushPull needs a list of callback functions.  Here we create a list
  // and add a function that doesn't do anything.	
	auto noopFunction = [](std::string const& str) {
  };
  typedef PushPull::FunctionType FunctionType;
  std::vector<FunctionType> functions;
  functions.push_back(noopFunction);

  PushPull* edgeCommunicator0 = new PushPull(numNodes, 0, numPushSockets,
                                             numPullThreads, hostnames, hwm,
                                             functions, startingPort, timeout,
                                             true);
  PushPull* edgeCommunicator1 = new PushPull(numNodes, 1, numPushSockets,
                                             numPullThreads, hostnames, hwm,
                                             functions, 
                                             startingPort, 
                                             timeout, true);
 
  MapType map0(numNodes, nodeId0, tableCapacity, edgeCommunicator0);
  MapType map1(numNodes, nodeId1, tableCapacity, edgeCommunicator1);
               
  // Two generators for each thread 
  std::shared_ptr<AbstractNetflowGenerator> generator0 = 
    std::make_shared<UniformDestPort>("192.168.0.0", 1);
  std::shared_ptr<AbstractNetflowGenerator> generator1 = 
    std::make_shared<UniformDestPort>("192.168.0.1", 1);

  EdgeRequestType edgeRequest0;
  EdgeRequestType edgeRequest1;

  // Make 0's edge request look for 1's assigned range
  edgeRequest0.setTarget("192.168.0.0");
  edgeRequest0.setReturn(1);

  // Make 1's edge request look for 0's assigned range
  edgeRequest1.setTarget("192.168.0.1");
  edgeRequest1.setReturn(0);

  map0.addRequest(edgeRequest0);
  map1.addRequest(edgeRequest1);
  
  size_t n = 10;

  auto mapFunction = [](MapType* map,
                        std::shared_ptr<AbstractNetflowGenerator> generator, 
                        size_t n,
                        size_t id)
  {
    LastOctetHashFunction hash;
    size_t i = 0;
    while (i < n) {
      std::string str = generator->generate();
      Netflow netflow = makeNetflow(i, str);
      DEBUG_PRINT("Node %lu processing netflow %s\n", id, 
        toString(netflow).c_str());
        
      map->process(netflow);

      // If the hash of the source ip equals the other node, then
      // we don't send the edge since it should have gotten it, so
      // we only increment the counter when we get a netflow where the source
      // hashes to the given node (thread).
      if (hash(std::get<SourceIp>(netflow)) % 2 == id) {
        i++;
				DEBUG_PRINT("Node %lu i %lu\n", id, i);
      }
    }

    map->terminate();

    DEBUG_PRINT("Node %lu Exiting PUSH thread\n", id);
  };

  /*auto pullFunction = [](
                         std::string url, 
                         int* count)
  {
    zmq::context_t context(1);
    auto socket = std::make_shared<zmq::socket_t>(context, ZMQ_PULL);
    int hwm = 1000;
    socket->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm));
    socket->connect(url);

    zmq::message_t message;
    bool stop = false;
    while(!stop) {
      socket->recv(&message);
      if (isTerminateMessage(message)) {

        DEBUG_PRINT("pull thread url %s terminate message\n", url.c_str());
        stop = true;

      } else {

        DEBUG_PRINT("pull thread url %s count %d\n", url.c_str(), *count);
        (*count)++;

      }
    }
    DEBUG_PRINT("Exiting PULL thread url%s\n", url.c_str());
  };*/


  std::string ip = getIpString("localhost");
  std::string baseurl = "tcp://" + ip + ":";
  std::string edgepull_url0;
  std::string edgepull_url1;
  edgepull_url0 = baseurl + 
    boost::lexical_cast<std::string>(startingPort);
  edgepull_url1 = baseurl + 
    boost::lexical_cast<std::string>(startingPort + 1);


  std::thread pushthread0(mapFunction, &map0, generator0, n, 0);
  std::thread pushthread1(mapFunction, &map1, generator1, n, 1);
  //int receiveCount0 = 0;
  //int receiveCount1 = 0;
  //std::thread pullthread0(pullFunction, 
  //                        edgepull_url1, &receiveCount0);
  //std::thread pullthread1(pullFunction, 
  //                        edgepull_url0, &receiveCount1);

  pushthread0.join();
  pushthread1.join();
  //pullthread0.join();
  //pullthread1.join();

  BOOST_CHECK_EQUAL(map0.getTotalEdgePushes(), n);
  BOOST_CHECK_EQUAL(map1.getTotalEdgePushes(), n);

	edgeCommunicator0->terminate();
	edgeCommunicator1->terminate();

  delete edgeCommunicator0;
  delete edgeCommunicator1;

}
