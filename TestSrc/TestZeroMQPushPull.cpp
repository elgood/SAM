#define BOOST_TEST_MAIN TestZeroMQPushPull

#define DEBUG

#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include "ZeroMQPushPull.hpp"
#include "NetflowGenerators.hpp"
#include <zmq.hpp>

using namespace sam;

typedef ZeroMQPushPull<Netflow, SourceIp, DestIp, NetflowTuplizer, 
                       StringHashFunction>
        PartitionType;

zmq::context_t context(1);

BOOST_AUTO_TEST_CASE( test_graph_store )
{
  int major, minor, patch;
  zmq_version(&major, &minor, &patch);
  std::cout << "ZMQ Version " << major << " " << minor << " " << patch 
            << std::endl;

  size_t queueLength = 1;
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

  size_t n = 10000;

  AbstractNetflowGenerator* generator0 = new UniformDestPort("192.168.0.1", 1);
  AbstractNetflowGenerator* generator1 = new UniformDestPort("192.168.0.2", 1);
   
  PartitionType* pushPull0 = new PartitionType(context, queueLength,
                                    numNodes, nodeId0,
                                    hostnames, ports,
                                    hwm);

  PartitionType* pushPull1 = new PartitionType(context, queueLength,
                                    numNodes, nodeId1,
                                    hostnames, ports,
                                    hwm);


  auto function = [n](AbstractNetflowGenerator *generator,
                      PartitionType* pushPull)
  {
    for(size_t i = 0; i < n; i++) {
      DEBUG_PRINT("Generating %luth netflow\n", i);
      std::string str = generator->generate();
      pushPull->consume(str);
    }
    pushPull->terminate();
  };

  pushPull0->acceptData();
  pushPull1->acceptData();

  std::thread thread0(function, generator0, pushPull0);
  std::thread thread1(function, generator1, pushPull1);

  thread0.join();
  thread1.join();

  BOOST_CHECK_EQUAL(n, pushPull0->getConsumeCount());
  BOOST_CHECK_EQUAL(n, pushPull1->getConsumeCount());

  BOOST_CHECK(4 * n > pushPull0->getNumReadItems() + 
                           pushPull1->getNumReadItems());

  delete pushPull0;
  delete pushPull1;
  delete generator0;
  delete generator1;
}

