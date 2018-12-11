#define BOOST_TEST_MAIN TestTriangles

//#define DEBUG

#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include "ZeroMQPushPull.hpp"
#include "NetflowGenerators.hpp"
#include "GraphStore.hpp"
#include "EdgeDescription.hpp"
#include "SubgraphQuery.hpp"
#include "FeatureMap.hpp"
#include <zmq.hpp>
#include <cstdlib>
#include <chrono>
#include <thread>

using namespace sam;
using namespace std::chrono;

typedef GraphStore<Netflow, NetflowTuplizer, SourceIp, DestIp,
                   TimeSeconds, DurationSeconds,
                   StringHashFunction, StringHashFunction,
                   StringEqualityFunction, StringEqualityFunction>
        GraphStoreType;

typedef GraphStoreType::QueryType SubgraphQueryType;
typedef GraphStoreType::ResultType ResultType;

typedef GraphStoreType::EdgeDescriptionType EdgeDescriptionType;

typedef ZeroMQPushPull<Netflow, SourceIp, DestIp, NetflowTuplizer, 
                       StringHashFunction>
        PartitionType;


//zmq::context_t context(1);

BOOST_AUTO_TEST_CASE( test_triangles_random_pool_of_vertices )
{
  printf("Starting test_triangles_random_pool_of_vertices\n");

  // Give time for last zmq threads to terminate from previous test
  std::this_thread::sleep_for(
    std::chrono::milliseconds(20000));

  /// This differs from the previous test in that when we generate netflows,
  /// we randomly select the source and destination from a small set of 
  /// vertices.  If there are n vertices, each edge is expected to 
  /// create (1/n^2) * w, where w is how many edges can occur in the window
  /// of time specified for the query.

  // Setting up random generators
  size_t numVertices = 500;
  AbstractNetflowGenerator *generator0 = new RandomPoolGenerator(numVertices);
  AbstractNetflowGenerator *generator1 = new RandomPoolGenerator(numVertices);
    

  // Setting up ZeroMQPushPull objects
  size_t queueLength = 1;
  size_t numNodes = 2;
  size_t nodeId0 = 0;
  size_t nodeId1 = 1;
  std::vector<std::string> hostnames;
  //std::vector<size_t> ports;
  size_t hwm = 1000;
  size_t startingPort = 10000;

  hostnames.push_back("localhost");
  //ports.push_back(10000);
  hostnames.push_back("localhost");
  //ports.push_back(10001);
  
  size_t numTuples = 10000;
  size_t timeout = 1000;

  // Sometimes it doesn't catch the triangles at the end because things 
  // terminate too quickly.  This adds a little buffer at the end where
  // no triangles occur.
  //size_t numExtra = 100;

  PartitionType* pushPull0 = new PartitionType(queueLength,
                                    numNodes, nodeId0,
                                    hostnames, //ports,
                                    startingPort, timeout, true,
                                    hwm);

  PartitionType* pushPull1 = new PartitionType(queueLength,
                                    numNodes, nodeId1,
                                    hostnames, //ports,
                                    startingPort, timeout, true,
                                    hwm);

  // Setting up GraphStore objects
  size_t graphCapacity = 1000; //For csc and csr
  size_t tableCapacity = 1000; //For SubgraphQueryResultMap intermediate results
  size_t resultsCapacity = 1000; //For final results
  double timeWindow = 1000;

  startingPort = 10002;
  size_t numPushSockets = 1;
  size_t numPullThreads = 1;
  double keepQueries = 1.0;

  auto featureMap = std::make_shared<FeatureMap>(1000);

  auto graphStore0 = std::make_shared<GraphStoreType>(
                          numNodes, nodeId0,
                          hostnames, startingPort, 
                          hwm, graphCapacity,
                          tableCapacity, resultsCapacity, 
                          numPushSockets, numPullThreads, timeout, 
                          timeWindow, keepQueries, featureMap, true);
                          

  startingPort += numPushSockets * (2 - 1) * 2;
  auto graphStore1 = std::make_shared<GraphStoreType>(
                          numNodes, nodeId1,
                          hostnames, startingPort,  
                          hwm, graphCapacity,
                          tableCapacity, resultsCapacity, 
                          numPushSockets, numPullThreads, timeout,
                          timeWindow, keepQueries, featureMap, true);


  // Set up GraphStore objects to get input from ZeroMQPushPull objects
  pushPull0->registerConsumer(graphStore0);
  pushPull1->registerConsumer(graphStore1);

  // Set up the triangle query
  EdgeFunction starttimeFunction = EdgeFunction::StartTime;
  EdgeFunction endtimeFunction = EdgeFunction::EndTime;
  EdgeOperator equal_edge_operator = EdgeOperator::Assignment;
  EdgeOperator greater_edge_operator = EdgeOperator::GreaterThan;
  EdgeOperator less_edge_operator = EdgeOperator::LessThan;

  std::string e0 = "e0";
  std::string e1 = "e1";
  std::string e2 = "e2";
  std::string nodex = "nodex";
  std::string nodey = "nodey";
  std::string nodez = "nodez";

  size_t queryTimeWindow = 10;
  EdgeExpression x2y(nodex, e0, nodey);
  EdgeExpression y2z(nodey, e1, nodez);
  EdgeExpression z2x(nodez, e2, nodex);
  TimeEdgeExpression startE0First(starttimeFunction, e0, 
                                  equal_edge_operator, 0);
  TimeEdgeExpression startE1First(starttimeFunction,e1,
                                  greater_edge_operator, 0);
  TimeEdgeExpression startE2First(starttimeFunction,e2,
                                  greater_edge_operator, 0);
  TimeEdgeExpression startE0Second(starttimeFunction, e0, 
                                   less_edge_operator,queryTimeWindow);
  TimeEdgeExpression startE1Second(starttimeFunction,e1,
                                   less_edge_operator,queryTimeWindow);
  TimeEdgeExpression startE2Second(starttimeFunction,e2,
                                   less_edge_operator, queryTimeWindow);


  SubgraphQueryType query(featureMap);
  query.addExpression(x2y);
  query.addExpression(y2z);
  query.addExpression(z2x);
  query.addExpression(startE0First);
  query.addExpression(startE1First);
  query.addExpression(startE2First);
  query.addExpression(startE0Second);
  query.addExpression(startE1Second);
  query.addExpression(startE2Second);
  query.finalize();

  graphStore0->registerQuery(query);
  graphStore1->registerQuery(query);

  // Checking that the query laid out how we expect
  EdgeDescriptionType const& edge0 = query.getEdgeDescription(0);
  EdgeDescriptionType const& edge1 = query.getEdgeDescription(1);
  EdgeDescriptionType const& edge2 = query.getEdgeDescription(2);

  BOOST_CHECK_EQUAL(edge0.source, nodex);
  BOOST_CHECK_EQUAL(edge1.source, nodey);
  BOOST_CHECK_EQUAL(edge2.source, nodez);

  double time = 0.0;
  double increment = 0.01;

  std::vector<Netflow> netflowList;
  std::mutex lock;

  //pushPull0->acceptData();
  //pushPull1->acceptData();

  // The lambda function
  auto generateFunction = [numTuples, &time, increment,
                           &netflowList, &lock]
                           (
                             PartitionType* pushPull,
                             AbstractNetflowGenerator* generator,
                             std::shared_ptr<GraphStoreType> graphStore,
                             size_t nodeId)
  {

    auto starttime = std::chrono::high_resolution_clock::now();
    AbstractNetflowGenerator *otherGenerator = 
      new RandomGenerator();
    for(size_t i = 0; i < numTuples; i++) {
      DEBUG_PRINT("NodeId %lu i %lu\n", nodeId, i);

      auto currenttime = std::chrono::high_resolution_clock::now();
      duration<double> diff = duration_cast<duration<double>>(
        currenttime - starttime);
      if (diff.count() < i * increment) {
        size_t numMilliseconds = (i * increment - diff.count()) * 1000;
        std::this_thread::sleep_for(
          std::chrono::milliseconds(numMilliseconds));
      }

      std::string str = generator->generate(time);
      time += increment;
      pushPull->consume(str);
      Netflow netflow = makeNetflow(i, str);
      lock.lock();
      netflowList.push_back(netflow);
      lock.unlock();
    }

    for(size_t i = 0; i < 1000; i++) {
      auto currenttime = std::chrono::high_resolution_clock::now();
      duration<double> diff = duration_cast<duration<double>>(
        currenttime - starttime);
      if (diff.count() < i * increment + numTuples * increment) {
        size_t numMilliseconds = (i * increment - diff.count()) * 1000;
        std::this_thread::sleep_for(
          std::chrono::milliseconds(numMilliseconds));
      } else {
        printf("behind by %f\n", diff.count() -i * increment + 
          numTuples * increment);
      }

     
      std::string str = otherGenerator->generate(time);
      time += increment;
      pushPull->consume(str);
    }

    pushPull->terminate();

    delete otherGenerator;

  };

  std::thread thread0(generateFunction, pushPull0, generator0, graphStore0, 0);
  std::thread thread1(generateFunction, pushPull1, generator1, graphStore1, 1);

  thread0.join();
  thread1.join();
  printf("Threads joined\n");

  size_t totalEdgePulls0 = graphStore0->getTotalEdgePulls();
  size_t totalEdgePulls1 = graphStore1->getTotalEdgePulls();
  size_t totalEdgePushes0 = graphStore0->getTotalEdgePushes();
  size_t totalEdgePushes1 = graphStore1->getTotalEdgePushes();
  printf("TotalEdgePushes0 %lu\n", totalEdgePushes0);
  printf("TotalEdgePushes1 %lu\n", totalEdgePushes1);
  printf("TotalEdgePulls0 %lu\n", totalEdgePulls0);
  printf("TotalEdgePulls1 %lu\n", totalEdgePulls1);

  BOOST_CHECK_EQUAL(totalEdgePulls0, totalEdgePushes1);
  BOOST_CHECK_EQUAL(totalEdgePulls1, totalEdgePushes0); 

  size_t totalRequestPulls0 = graphStore0->getTotalRequestPulls();
  size_t totalRequestPulls1 = graphStore1->getTotalRequestPulls();
  size_t totalRequestPushes0 = graphStore0->getTotalRequestPushes();
  size_t totalRequestPushes1 = graphStore1->getTotalRequestPushes();
  printf("TotalRequestPushes0 %lu\n", totalRequestPushes0);
  printf("TotalRequestPushes1 %lu\n", totalRequestPushes1);
  printf("TotalRequestPulls0 %lu\n", totalRequestPulls0);
  printf("TotalRequestPulls1 %lu\n", totalRequestPulls1);
  BOOST_CHECK_EQUAL(totalRequestPulls0, totalRequestPushes1);
  BOOST_CHECK_EQUAL(totalRequestPulls1, totalRequestPushes0);

  size_t totalResults = graphStore0->getNumResults() +
                        graphStore1->getNumResults();

  std::cout << "GraphStore0 num results " << graphStore0->getNumResults() 
            << std::endl;
  std::cout << "GraphStore1 num results " << graphStore1->getNumResults()
            << std::endl;
  std::cout << "Total results " << totalResults << std::endl;

  size_t numGetResults0 = (graphStore0->getNumResults() < resultsCapacity) ? 
                          graphStore0->getNumResults() : resultsCapacity; 
  size_t numGetResults1 = (graphStore1->getNumResults() < resultsCapacity) ? 
                          graphStore1->getNumResults() : resultsCapacity; 

  for(size_t i = 0; i < numGetResults0; i++) {
    ResultType result = graphStore0->getResult(i);
    BOOST_CHECK(result.complete());
    Netflow n0 = result.getResultTuple(0); 
    Netflow n1 = result.getResultTuple(1); 
    Netflow n2 = result.getResultTuple(2); 
    double starttime0 = std::get<TimeSeconds>(n0);
    double starttime1 = std::get<TimeSeconds>(n1);
    double starttime2 = std::get<TimeSeconds>(n2);
    BOOST_CHECK(starttime0 <= starttime1);
    BOOST_CHECK(starttime1 <= starttime2);
    BOOST_CHECK(starttime2 - starttime0 < queryTimeWindow);
  }

  for(size_t i = 0; i < numGetResults1; i++) {
    ResultType result = graphStore1->getResult(i);
    BOOST_CHECK(result.complete());
    Netflow n0 = result.getResultTuple(0); 
    Netflow n1 = result.getResultTuple(1); 
    Netflow n2 = result.getResultTuple(2); 
    double starttime0 = std::get<TimeSeconds>(n0);
    double starttime1 = std::get<TimeSeconds>(n1);
    double starttime2 = std::get<TimeSeconds>(n2);
    BOOST_CHECK(starttime0 <= starttime1);
    BOOST_CHECK(starttime1 <= starttime2);
    BOOST_CHECK(starttime2 - starttime0 < queryTimeWindow);
  }

  size_t calculatedNumTriangles = 
    sam::numTriangles<Netflow, SourceIp, DestIp, TimeSeconds,
                      DurationSeconds>(netflowList, queryTimeWindow);

  BOOST_CHECK_EQUAL(calculatedNumTriangles, totalResults);

  delete pushPull0;
  delete pushPull1;
  delete generator0;
  delete generator1;
}

