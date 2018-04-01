#define BOOST_TEST_MAIN TestEdgeRequestList

#include <boost/test/unit_test.hpp>
#include "EdgeRequestMap.hpp"
#include "NetflowGenerators.hpp"
#include <thread>

using namespace sam;




typedef EdgeRequestMap<Netflow, SourceIp, DestIp,
  LastOctetHashFunction, LastOctetHashFunction,
  StringEqualityFunction, StringEqualityFunction> MapType;

typedef MapType::EdgeRequestType EdgeRequestType;


BOOST_AUTO_TEST_CASE( test_edge_request_map )
{
  size_t numNodes = 2;
  size_t nodeId0 = 0;
  size_t nodeId1 = 1;
  std::vector<std::string> edgeHostnames;
  edgeHostnames.push_back("localhost");
  edgeHostnames.push_back("localhost");
  std::vector<size_t> edgePorts;
  edgePorts.push_back(10000);
  edgePorts.push_back(10001);
  uint32_t hwm = 1000;
  size_t tableCapacity = 1000;

  MapType map0(numNodes, nodeId0, edgeHostnames, edgePorts, hwm, 
                      tableCapacity);
  MapType map1(numNodes, nodeId1, edgeHostnames, edgePorts, hwm, 
                      tableCapacity);


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
  
  size_t n = 1000;


  auto mapFunction = [](MapType* map,
                        std::shared_ptr<AbstractNetflowGenerator> generator, 
                        size_t n)
  {
    for (size_t i = 0; i < n; i++) {
      std::string str = generator->generate();
      Netflow netflow = makeNetflow(i, str);
      map->process(netflow);
    }
    map->terminate();
    #ifdef DEBUG
    printf("Exiting PUSH thread\n");
    #endif
  };

  auto pullFunction = [](
                         std::string url, 
                         int* count)
  {
    zmq::context_t context(1);
    auto socket = std::make_shared<zmq::socket_t>(context, ZMQ_PULL);
    int hwm = 1000;
    socket->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm));
    socket->bind(url);

    zmq::message_t message;
    bool stop = false;
    while(!stop) {
      socket->recv(&message);
      if (isTerminateMessage(message)) {
        #ifdef DEBUG
        printf("pull thread url %s TERMINATE message\n", url.c_str());
        #endif
        stop = true;
      } else {
        #ifdef DEBUG
        printf("pull thread url %s count %d\n", url.c_str(), *count);
        #endif
        (*count)++;
      }
    }
    #ifdef DEBUG
    printf("Existing PULL thread url%s\n", url.c_str());
    #endif
  };


  std::string ip = getIpString("localhost");
  std::string baseurl = "tcp://" + ip + ":";
  std::string edgepull_url0;
  std::string edgepull_url1;
  edgepull_url0 = baseurl + 
    boost::lexical_cast<std::string>(edgePorts[0]);
  edgepull_url1 = baseurl + 
    boost::lexical_cast<std::string>(edgePorts[1]);


  std::thread pushthread0(mapFunction, &map0, generator0, n);
  std::thread pushthread1(mapFunction, &map1, generator1, n);
  int receiveCount0 = 0;
  int receiveCount1 = 0;
  std::thread pullthread0(pullFunction, 
                          edgepull_url1, &receiveCount0);
  std::thread pullthread1(pullFunction, 
                          edgepull_url0, &receiveCount1);

  //edgePushers[0]->send(emptyZmqMessage());
  //edgePushers[1]->send(emptyZmqMessage());

  pushthread0.join();
  pushthread1.join();
  pullthread0.join();
  pullthread1.join();

  BOOST_CHECK_EQUAL(map0.getTotalEdgePushes(), n);
  BOOST_CHECK_EQUAL(map1.getTotalEdgePushes(), n);

  //printf("At the end\n");

}
