#define BOOST_TEST_MAIN TestGraphStore
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include "GraphStore.hpp"
#include "NetflowGenerators.hpp"
#include <zmq.hpp>

using namespace sam;

typedef GraphStore<Netflow, NetflowTuplizer, SourceIp, DestIp, 
                   TimeSeconds, DurationSeconds, 
                   StringHashFunction, StringHashFunction, 
                   StringEqualityFunction, StringEqualityFunction>
        GraphStoreType;

BOOST_AUTO_TEST_CASE( test_graph_store )
{
  /// In this test we create a graphstore on two nodes (both local addresses).
  ///
  int major, minor, patch;
  zmq_version(&major, &minor, &patch);
  std::cout << "ZMQ Version " << major << " " << minor << " " << patch 
            << std::endl;

  size_t numNodes = 2;
  size_t nodeId0 = 0;
  size_t nodeId1 = 1;
  std::vector<std::string> requestHostnames;
  std::vector<size_t> requestPorts;
  std::vector<std::string> edgeHostnames;
  std::vector<size_t> edgePorts;
  size_t hwm = 1000;
  size_t graphCapacity = 1000; //For csc and csr
  size_t tableCapacity = 1000; //For SubgraphQueryResultMap intermediate results
  size_t resultsCapacity = 1000; //For final results
  double timeWindow = 100;

  requestHostnames.push_back("localhost");
  requestPorts.push_back(10000);
  requestHostnames.push_back("localhost");
  requestPorts.push_back(10001);
  edgeHostnames.push_back("localhost");
  edgePorts.push_back(10002);
  edgeHostnames.push_back("localhost");
  edgePorts.push_back(10003);

  int n = 1000;

  GraphStoreType* graphStore0 = new GraphStoreType(numNodes, nodeId0, 
                          requestHostnames, requestPorts,
                          edgeHostnames, edgePorts,
                          hwm, graphCapacity, 
                          tableCapacity, resultsCapacity, timeWindow); 


  // One thread runs this.
  auto graph_function0 = [graphStore0, n]()
                          
  {
    AbstractNetflowGenerator *generator0 = 
      new UniformDestPort("192.168.0.1", 1);
    
    for (int i = 0; i < n; i++) {
      std::string str = generator0->generate();
      Netflow n = makeNetflow(0, str);
      graphStore0->consume(n);
    }
    graphStore0->terminate();

    
    delete generator0;
  };

  GraphStoreType* graphStore1 = new GraphStoreType(numNodes, nodeId1, 
                          requestHostnames, requestPorts,
                          edgeHostnames, edgePorts,
                          hwm, graphCapacity, 
                          tableCapacity, resultsCapacity, timeWindow); 

  // Another thread runs this.
  auto graph_function1 = [graphStore1, n]()
                          
  {
    AbstractNetflowGenerator *generator1 = 
      new UniformDestPort("192.168.0.2", 1);
    
    for (int i = 0; i < n; i++) {
      std::string str = generator1->generate();
      Netflow n = makeNetflow(0, str);
      graphStore1->consume(n);
    }
    graphStore1->terminate();

    
    delete generator1;
  };

  std::thread thread0(graph_function0);
  std::thread thread1(graph_function1);

  thread0.join();
  thread1.join();

  BOOST_CHECK_EQUAL(graphStore1->getTuplesReceived(), n);
}

