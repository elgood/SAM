#define BOOST_TEST_MAIN TestZeroMQPushPull
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include "ZeroMQPushPull.hpp"
#include "NetflowGenerators.hpp"
#include <zmq.hpp>

using namespace sam;

BOOST_AUTO_TEST_CASE( test_graph_store )
{
  int major, minor, patch;
  zmq_version(&major, &minor, &patch);
  std::cout << "ZMQ Version " << major << " " << minor << " " << patch 
            << std::endl;

  size_t queueLength = 100;
  size_t numNodes = 2;
  size_t nodeId0 = 0;
  size_t nodeId1 = 1;
  std::vector<std::string> hostnames;
  std::vector<size_t> ports;
  size_t hwm = 1000;

  hostnames.push_back("localhost");
  ports.push_back(10000);
  hostnames.push_back("localhost");
  ports.push_back(10001);

  int n = 10;

  ZeroMQPushPull* pushPull0 = new ZeroMQPushPull(queueLength,
                                    numNodes, nodeId0,
                                    hostnames, ports,
                                    hwm);

  auto function0 = [pushPull0, n]()
                          
  {
    AbstractNetflowGenerator *generator0 = 
      new UniformDestPort("192.168.0.1", 1);
    
    for (int i = 0; i < n; i++) {
      std::string str = generator0->generate();
      pushPull0->consume(str);
    }
    pushPull0->terminate();
    
    delete generator0;
  };

  ZeroMQPushPull* pushPull1 = new ZeroMQPushPull(queueLength,
                                    numNodes, nodeId1,
                                    hostnames, ports,
                                    hwm);

  auto function1 = [pushPull1, n]()
                          
  {
    AbstractNetflowGenerator *generator1 = 
      new UniformDestPort("192.168.0.2", 1);
    
    for (int i = 0; i < n; i++) {
      std::string str = generator1->generate();
      pushPull1->consume(str);
    }
    pushPull1->terminate();
    
    delete generator1;
  };


  std::thread thread0(function0);
  std::thread thread1(function1);

  thread0.join();
  thread1.join();

  BOOST_CHECK_EQUAL(n, pushPull0->getConsumeCount());
  BOOST_CHECK_EQUAL(n, pushPull1->getConsumeCount());

  BOOST_CHECK(4 * n > pushPull0->getNumReadItems() + 
                           pushPull1->getNumReadItems());

  delete pushPull0;
  delete pushPull1;

}

