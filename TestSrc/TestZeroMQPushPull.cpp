#define BOOST_TEST_MAIN TestZeroMQPushPull

//#define DEBUG

#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include <sam/ZeroMQPushPull.hpp>
#include <sam/VastNetflowGenerators.hpp>
#include <sam/VastNetflow.hpp>
#include <sam/Util.hpp>
#include <zmq.hpp>

using namespace sam;

typedef TupleStringHashFunction<VastNetflow, SourceIp> SourceHash;
typedef TupleStringHashFunction<VastNetflow, DestIp> TargetHash;

typedef ZeroMQPushPull<VastNetflow, VastNetflowTuplizer, SourceHash, TargetHash>
        PartitionType;

zmq::context_t context(1);

BOOST_AUTO_TEST_CASE( test_zeromqpushpull )
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
  size_t hwm = 1000;
  size_t timeout = 1000;
  size_t startingPort = 10000;

  hostnames.push_back("localhost");
  hostnames.push_back("localhost");

  size_t n = 10000;

  // These two generators create a bunch of netflows with
  // random source ips.  One set of netflows go towards
  // 192.168.0.1 on one port and the other set of netflows
  // goes to 192.168.0.2 on one port.  
  AbstractVastNetflowGenerator* generator0 = 
    new UniformDestPort("192.168.0.1", 1);
  AbstractVastNetflowGenerator* generator1 = 
    new UniformDestPort("192.168.0.2", 1);
   
  PartitionType* pushPull0 = new PartitionType(queueLength,
                                    numNodes, nodeId0,
                                    hostnames, startingPort, 
                                    timeout, true, hwm);

  PartitionType* pushPull1 = new PartitionType(queueLength,
                                    numNodes, nodeId1,
                                    hostnames, startingPort, 
                                    timeout, true, hwm);


  auto function = [n](AbstractVastNetflowGenerator *generator,
                      PartitionType* pushPull)
  {
    for(size_t i = 0; i < n; i++) {
      DEBUG_PRINT("Generating %luth netflow\n", i);
      std::string str = generator->generate();
      pushPull->consume(str);
    }
    pushPull->terminate();
  };

  std::thread thread0(function, generator0, pushPull0);
  std::thread thread1(function, generator1, pushPull1);

  thread0.join();
  thread1.join();

  BOOST_CHECK_EQUAL(n, pushPull0->getConsumeCount());
  BOOST_CHECK_EQUAL(n, pushPull1->getConsumeCount());

  DEBUG_PRINT("pushPull0->getNumReadItems %lu "
    "pushPull1->getNumReadItems %lu "
    "4 * n %lu\n",
    pushPull0->getNumReadItems(),
    pushPull1->getNumReadItems(),
    4*n);

  // We create n netflows on each generator.  There are two generators.
  // Thus there are 2 * n netflows.  However, each Partitioner locally
  // consumes the netflow, or it can also send the netflow to the other
  // partitioner, resulting 2n <= total number of items read << 4n.
  BOOST_CHECK(2 * n <= pushPull0->getNumReadItems() + 
                           pushPull1->getNumReadItems());
  BOOST_CHECK(4 * n >= pushPull0->getNumReadItems() + 
                           pushPull1->getNumReadItems());

  delete pushPull0;
  delete pushPull1;
  delete generator0;
  delete generator1;
}

