#define BOOST_TEST_MAIN TestTestProducers
#include <boost/test/unit_test.hpp>
#include <iostream>
#include <sam/TestProducers.hpp>

using namespace sam;

typedef VastNetflow TupleType;
typedef EmptyLabel LabelType;
typedef Edge<size_t, LabelType, TupleType> EdgeType;
typedef TuplizerFunction<EdgeType, MakeVastNetflow> Tuplizer;
typedef PopularSites<EdgeType, Tuplizer> PopularSitesType;
typedef TopKProducer<EdgeType, Tuplizer> TopKProducerType;

BOOST_AUTO_TEST_CASE( test_topkproducer )
{
  size_t nodeId = 0;
  int queueLength = 1000;
  int numExamples = 100000;
  int numServers = 2;
  int numNonservers = 2;
  TopKProducerType producer(nodeId, queueLength, numExamples, numServers, 
                        numNonservers);

  producer.run();

  auto ipPortMap = producer.getIpPortMap();
  
  int count = ipPortMap[std::pair<std::string, int>("192.168.0.1",1)];
  BOOST_CHECK_CLOSE(static_cast<double>(count)/numExamples, 0.5, 0.01);

  count = ipPortMap[std::pair<std::string, int>("192.168.0.1",2)];
  BOOST_CHECK_CLOSE(static_cast<double>(count)/numExamples, 0.5, 0.01);
  
  count = ipPortMap[std::pair<std::string, int>("192.168.0.2",1)];
  BOOST_CHECK_CLOSE(static_cast<double>(count)/numExamples, 0.5, 0.01);

  count = ipPortMap[std::pair<std::string, int>("192.168.0.2",2)];
  BOOST_CHECK_CLOSE(static_cast<double>(count)/numExamples, 0.5, 0.01);

  count = ipPortMap[std::pair<std::string, int>("192.168.0.3",1)];
  BOOST_CHECK_CLOSE(static_cast<double>(count)/numExamples, 0.3333, 0.05);

  count = ipPortMap[std::pair<std::string, int>("192.168.0.3",2)];
  BOOST_CHECK_CLOSE(static_cast<double>(count)/numExamples, 0.3333, 0.05);
  
  count = ipPortMap[std::pair<std::string, int>("192.168.0.3",3)];
  BOOST_CHECK_CLOSE(static_cast<double>(count)/numExamples, 0.3333, 0.05);
  
  count = ipPortMap[std::pair<std::string, int>("192.168.0.4",1)];
  BOOST_CHECK_CLOSE(static_cast<double>(count)/numExamples, 0.3333, 0.05);

  count = ipPortMap[std::pair<std::string, int>("192.168.0.4",2)];
  BOOST_CHECK_CLOSE(static_cast<double>(count)/numExamples, 0.3333, 0.05);

  count = ipPortMap[std::pair<std::string, int>("192.168.0.4",3)];
  BOOST_CHECK_CLOSE(static_cast<double>(count)/numExamples, 0.3333, 0.05);

}
