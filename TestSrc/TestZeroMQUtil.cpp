#define BOOST_TEST_MAIN TestUtil
#include <boost/test/unit_test.hpp>
#include <boost/tokenizer.hpp>
#include <stdexcept>
#include <tuple>
#include <string>
#include <random>
#include "ZeroMQUtil.hpp"
#include "Netflow.hpp"

using namespace sam;

BOOST_AUTO_TEST_CASE( test_empty_zmq_message)
{
  zmq::message_t message = emptyZmqMessage();
  BOOST_CHECK_EQUAL(message.size(), 0);

  BOOST_CHECK(isTerminateMessage(message));

}

BOOST_AUTO_TEST_CASE( test_createpushsockets )
{
  zmq::context_t context(1);
  std::vector<std::string> hostnames;
  std::vector<size_t> ports;
  std::vector<std::shared_ptr<zmq::socket_t>> pushers;

  hostnames.push_back("localhost");
  hostnames.push_back("localhost");
  ports.push_back(10000);
  ports.push_back(10001);

  int hwm = 1000;

  size_t numNodes = 2;
  size_t nodeId = 0;

  createPushSockets(&context, numNodes, nodeId, hostnames, ports, pushers, hwm);

}

BOOST_AUTO_TEST_CASE( tests_getPortForPull )
{
  size_t numPushSockets = 2;
  size_t numNodes = 5;
  size_t nodeId = 0;
  size_t startingPort = 10000;

  // Will throw exception because index is past (numNodes -1) * numPushSockets;
  BOOST_CHECK_THROW(getPortForPull((numNodes-1)*numPushSockets, 
    nodeId, numPushSockets, numNodes, startingPort), ZeroMQUtilException);

  BOOST_CHECK_EQUAL(startingPort, 
    getPortForPull(0, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 1, 
    getPortForPull(1, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort, 
    getPortForPull(2, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 1, 
    getPortForPull(3, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort, 
    getPortForPull(4, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 1, 
    getPortForPull(5, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort, 
    getPortForPull(6, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 1, 
    getPortForPull(7, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_THROW( 
    getPortForPull(8, nodeId, numPushSockets, numNodes, startingPort),
    ZeroMQUtilException); 


  nodeId = 1; 
  BOOST_CHECK_EQUAL(startingPort, 
    getPortForPull(0, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 1, 
    getPortForPull(1, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + numPushSockets, 
    getPortForPull(2, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + numPushSockets + 1, 
    getPortForPull(3, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + numPushSockets, 
    getPortForPull(4, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + numPushSockets + 1, 
    getPortForPull(5, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + numPushSockets, 
    getPortForPull(6, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + numPushSockets + 1, 
    getPortForPull(7, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_THROW( 
    getPortForPull(8, nodeId, numPushSockets, numNodes, startingPort),
    ZeroMQUtilException); 

  nodeId = 2; 
  BOOST_CHECK_EQUAL(startingPort + numPushSockets, 
    getPortForPull(0, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + numPushSockets + 1, 
    getPortForPull(1, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + numPushSockets, 
    getPortForPull(2, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + numPushSockets + 1, 
    getPortForPull(3, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 2 * numPushSockets, 
    getPortForPull(4, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 2 * numPushSockets + 1, 
    getPortForPull(5, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 2 * numPushSockets, 
    getPortForPull(6, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 2 * numPushSockets + 1, 
    getPortForPull(7, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_THROW( 
    getPortForPull(8, nodeId, numPushSockets, numNodes, startingPort),
    ZeroMQUtilException); 

  nodeId = 3; 
  BOOST_CHECK_EQUAL(startingPort + 2 * numPushSockets, 
    getPortForPull(0, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 2 * numPushSockets + 1, 
    getPortForPull(1, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 2 * numPushSockets, 
    getPortForPull(2, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 2 * numPushSockets + 1, 
    getPortForPull(3, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 2 * numPushSockets, 
    getPortForPull(4, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 2 * numPushSockets + 1, 
    getPortForPull(5, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 3 * numPushSockets, 
    getPortForPull(6, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 3 * numPushSockets + 1, 
    getPortForPull(7, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_THROW( 
    getPortForPull(8, nodeId, numPushSockets, numNodes, startingPort),
    ZeroMQUtilException); 

  nodeId = 4; 
  BOOST_CHECK_EQUAL(startingPort + 3 * numPushSockets, 
    getPortForPull(0, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 3 * numPushSockets + 1, 
    getPortForPull(1, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 3 * numPushSockets, 
    getPortForPull(2, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 3 * numPushSockets + 1, 
    getPortForPull(3, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 3 * numPushSockets, 
    getPortForPull(4, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 3 * numPushSockets + 1, 
    getPortForPull(5, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 3 * numPushSockets, 
    getPortForPull(6, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_EQUAL(startingPort + 3 * numPushSockets + 1, 
    getPortForPull(7, nodeId, numPushSockets, numNodes, startingPort)); 
  BOOST_CHECK_THROW( 
    getPortForPull(8, nodeId, numPushSockets, numNodes, startingPort),
    ZeroMQUtilException); 




 
}

BOOST_AUTO_TEST_CASE( test_getHostnameForPull )
{
  size_t numPushSockets = 2;
  size_t numNodes = 5;
  size_t nodeId = 0;

  std::vector<std::string> hostnames;
  // leave one out at the end to induce exception
  for(size_t i = 0; i < numNodes - 1; i++) {
    hostnames.push_back("node" + boost::lexical_cast<std::string>(i));
  }

  // Will throw exception because index is past (numNodes -1) * numPushSockets;
  BOOST_CHECK_THROW(getHostnameForPull((numNodes-1)*numPushSockets, 
    nodeId, numPushSockets, numNodes, hostnames), ZeroMQUtilException);
  // Will throw an exception because hostnames isn't the proper size.
  BOOST_CHECK_THROW(getHostnameForPull((numNodes-1)*numPushSockets - 1, 
    nodeId, numPushSockets, numNodes, hostnames), ZeroMQUtilException);

  hostnames.push_back("node" + boost::lexical_cast<std::string>(numNodes - 1));

  BOOST_CHECK_EQUAL("node1", 
    getHostnameForPull(0, nodeId, numPushSockets, numNodes, hostnames));
  BOOST_CHECK_EQUAL("node1", 
    getHostnameForPull(1, nodeId, numPushSockets, numNodes, hostnames));
  BOOST_CHECK_EQUAL("node2", 
    getHostnameForPull(2, nodeId, numPushSockets, numNodes, hostnames));
  BOOST_CHECK_EQUAL("node2", 
    getHostnameForPull(3, nodeId, numPushSockets, numNodes, hostnames));
  BOOST_CHECK_EQUAL("node3", 
    getHostnameForPull(4, nodeId, numPushSockets, numNodes, hostnames));
  BOOST_CHECK_EQUAL("node3", 
    getHostnameForPull(5, nodeId, numPushSockets, numNodes, hostnames));
  BOOST_CHECK_EQUAL("node4", 
    getHostnameForPull(6, nodeId, numPushSockets, numNodes, hostnames));
  BOOST_CHECK_EQUAL("node4", 
    getHostnameForPull(7, nodeId, numPushSockets, numNodes, hostnames));
  BOOST_CHECK_THROW(getHostnameForPull(8, nodeId, numPushSockets, numNodes,
                    hostnames), ZeroMQUtilException);


  nodeId = 1;
  BOOST_CHECK_EQUAL("node0", 
    getHostnameForPull(0, nodeId, numPushSockets, numNodes, hostnames));
  BOOST_CHECK_EQUAL("node0", 
    getHostnameForPull(1, nodeId, numPushSockets, numNodes, hostnames));
  BOOST_CHECK_EQUAL("node2", 
    getHostnameForPull(2, nodeId, numPushSockets, numNodes, hostnames));
  BOOST_CHECK_EQUAL("node2", 
    getHostnameForPull(3, nodeId, numPushSockets, numNodes, hostnames));
  BOOST_CHECK_EQUAL("node3", 
    getHostnameForPull(4, nodeId, numPushSockets, numNodes, hostnames));
  BOOST_CHECK_EQUAL("node3", 
    getHostnameForPull(5, nodeId, numPushSockets, numNodes, hostnames));
  BOOST_CHECK_EQUAL("node4", 
    getHostnameForPull(6, nodeId, numPushSockets, numNodes, hostnames));
  BOOST_CHECK_EQUAL("node4", 
    getHostnameForPull(7, nodeId, numPushSockets, numNodes, hostnames));
  BOOST_CHECK_THROW(getHostnameForPull(8, nodeId, numPushSockets, numNodes,
                    hostnames), ZeroMQUtilException);

  nodeId = 2;
  BOOST_CHECK_EQUAL("node0", 
    getHostnameForPull(0, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node0", 
    getHostnameForPull(1, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node1", 
    getHostnameForPull(2, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node1", 
    getHostnameForPull(3, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node3", 
    getHostnameForPull(4, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node3", 
    getHostnameForPull(5, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node4", 
    getHostnameForPull(6, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node4", 
    getHostnameForPull(7, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_THROW(getHostnameForPull(8, nodeId, numPushSockets, numNodes,
                    hostnames), ZeroMQUtilException);


  nodeId = 3;
  BOOST_CHECK_EQUAL("node0", 
    getHostnameForPull(0, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node0", 
    getHostnameForPull(1, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node1", 
    getHostnameForPull(2, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node1", 
    getHostnameForPull(3, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node2", 
    getHostnameForPull(4, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node2", 
    getHostnameForPull(5, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node4", 
    getHostnameForPull(6, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node4", 
    getHostnameForPull(7, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_THROW(getHostnameForPull(8, nodeId, numPushSockets, numNodes,
                    hostnames), ZeroMQUtilException);


  nodeId = 4;
  BOOST_CHECK_EQUAL("node0", 
    getHostnameForPull(0, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node0", 
    getHostnameForPull(1, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node1", 
    getHostnameForPull(2, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node1", 
    getHostnameForPull(3, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node2", 
    getHostnameForPull(4, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node2", 
    getHostnameForPull(5, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node3", 
    getHostnameForPull(6, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_EQUAL("node3", 
    getHostnameForPull(7, nodeId, numPushSockets, numNodes,
                    hostnames));
  BOOST_CHECK_THROW(getHostnameForPull(8, nodeId, numPushSockets, numNodes,
                    hostnames), ZeroMQUtilException);




}
