
#define BOOST_TEST_MAIN TestWateringHole
#include <boost/test/unit_test.hpp>
#include <chrono>
#include "GraphStore.hpp"
#include "NetflowGenerators.hpp"
#include "ZeroMQPushPull.hpp"
#include "TopK.hpp"

#define DEBUG

using namespace sam;
using namespace std::chrono;

typedef GraphStore<Netflow, NetflowTuplizer, SourceIp, DestIp,
                   TimeSeconds, DurationSeconds,
                   StringHashFunction, StringHashFunction,
                   StringEqualityFunction, StringEqualityFunction>
        GraphStoreType;

typedef GraphStoreType::QueryType SubgraphQueryType;
typedef GraphStoreType::ResultType ResultType;

typedef ZeroMQPushPull<Netflow, SourceIp, DestIp,
        NetflowTuplizer, StringHashFunction>
        PartitionType;


BOOST_AUTO_TEST_CASE( test_watering_hole )
{
  size_t numClients = 1000;
  size_t numServers = 5;
  size_t numNetflows = 10000;

  WateringHoleGenerator generator(numClients, numServers);

  /////////////// Setting up Partition object ///////////////////////////
  size_t numNodes = 1;
  size_t nodeId0 = 0;
  std::vector<std::string> hostnames;
  hostnames.push_back("localhost");
  size_t startingPort = 10000;
  size_t timeout = 1000;
  size_t hwm = 1000;
  size_t queueLength = 1;
  
  auto pushPull = std::make_shared<PartitionType>(queueLength, 
                                                  numNodes, nodeId0,
                                                  hostnames, 
                                                  startingPort, timeout, true,
                                                  hwm);


  ////////////////// Setting up topk operator /////////////////////
  size_t capacity = 100000;
  auto featureMap = std::make_shared<FeatureMap>(capacity);
  size_t N; ///> The total number of elements in a sliding window
  size_t b; ///> The number of elements in a dormant or active window
  size_t k; ///> The number of elements to keep track of
  std::string topkId = "topk";
  auto topk = std::make_shared<
    TopK<Netflow, DestIp>>(N, b, k, nodeId0, featureMap, topkId);

  pushPull->registerConsumer(topk);

  /////////////// Setting up GraphStore /////////////////////////////////
  size_t graphCapacity = 1000; //For csc and csr
  size_t tableCapacity = 1000; //For SubgraphQueryResultMap intermediate results
  size_t resultsCapacity = 1000; //For final results
  double timeWindow = 10000;
  size_t numPushSockets = 1;
  size_t numPullThreads = 1;
  double keepQueries = 1.0;

  auto graphStore = std::make_shared<GraphStoreType>( numNodes, nodeId0,
                                             hostnames, startingPort,
                                             hwm, graphCapacity,
                                             tableCapacity, resultsCapacity,
                                             numPushSockets, numPullThreads,
                                             timeout, timeWindow, keepQueries,
                                             featureMap);

  pushPull->registerConsumer(graphStore);

  /////////////// The Watering Hole query ///////////////////////////////
  std::string e0 = "e0";
  std::string e1 = "e1";
  std::string bait = "bait";
  std::string target = "target";
  std::string controller = "controller";

  // Set up the query
  EdgeFunction starttimeFunction = EdgeFunction::StartTime;
  EdgeFunction endtimeFunction = EdgeFunction::EndTime;
  EdgeOperator greaterEdgeOperator = EdgeOperator::GreaterThan;
  EdgeOperator lessEdgeOperator = EdgeOperator::LessThan;
  EdgeOperator equalEdgeOperator = EdgeOperator::Assignment;

  EdgeExpression target2Bait(target, e0, bait);
  EdgeExpression target2Controller(target, e1, controller);
  TimeEdgeExpression endE0Second(endtimeFunction, e0,
                                 equalEdgeOperator, 0);
  TimeEdgeExpression startE1First(starttimeFunction, e1,
                                  greaterEdgeOperator, 0);
  TimeEdgeExpression startE1Second(starttimeFunction, e1,
                                  lessEdgeOperator, 10);

  //bait in Top1000
  VertexConstraintExpression baitTopK(bait, VertexOperator::In, topkId);
  
  //controller not in Top1000
  VertexConstraintExpression controllerNotTopK(controller, 
                                               VertexOperator::NotIn, topkId);

  SubgraphQueryType query;
  query.addExpression(target2Bait);
  query.addExpression(target2Controller);
  query.addExpression(endE0Second);
  query.addExpression(startE1First);
  query.addExpression(startE1Second);
  query.addExpression(baitTopK);
  query.addExpression(controllerNotTopK);
  query.finalize();

  graphStore->registerQuery(query);

  double time = 0.0;
  double increment = 0.01;

  size_t numBadMessages = 5;
 
  auto starttime = std::chrono::high_resolution_clock::now();
  // Sending benign message
  for (size_t i = 0; i < numNetflows; i++) 
  {
    auto currenttime = std::chrono::high_resolution_clock::now();
    duration<double> diff = duration_cast<duration<double>>(
      currenttime - starttime);
    if (diff.count() < i * increment) {
      size_t numMilliseconds = (i * increment - diff.count()) * 1000;
      std::this_thread::sleep_for(
        std::chrono::milliseconds(numMilliseconds));
    }

    std::string str = generator.generate(time);
    Netflow netflow = makeNetflow(i, str);
    time += increment;
    graphStore->consume(netflow);
  }

  // Sending malicious messages
  for (size_t i = 0; i < numBadMessages; i++)
  {
    std::string str = generator.generateControlMessage(time);
    Netflow netflow = makeNetflow(i, str);
    time += increment;
    graphStore->consume(netflow);
  }

  BOOST_CHECK_EQUAL(graphStore->getNumResults(), numBadMessages);

}
