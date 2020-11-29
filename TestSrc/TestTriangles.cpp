#define BOOST_TEST_MAIN TestTriangles

//#define DEBUG

#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include <sam/ZeroMQPushPull.hpp>
#include <sam/tuples/VastNetflowGenerators.hpp>
#include <sam/tuples/Edge.hpp>
#include <sam/tuples/Tuplizer.hpp>
#include <sam/GraphStore.hpp>
#include <sam/EdgeDescription.hpp>
#include <sam/SubgraphQuery.hpp>
#include <zmq.hpp>
#include <cstdlib>
#include <chrono>
#include <thread>

using namespace sam;
using namespace sam::vast_netflow;
using namespace std::chrono;

typedef VastNetflow TupleType;
typedef EmptyLabel LabelType;
typedef Edge<size_t, LabelType, TupleType> EdgeType;
typedef TuplizerFunction<EdgeType, MakeVastNetflow> Tuplizer; 
typedef GraphStore<EdgeType, Tuplizer, SourceIp, DestIp,
                   TimeSeconds, DurationSeconds,
                   StringHashFunction, StringHashFunction,
                   StringEqualityFunction, StringEqualityFunction>
        GraphStoreType;

typedef GraphStoreType::QueryType SubgraphQueryType;
typedef GraphStoreType::ResultType ResultType;

typedef GraphStoreType::EdgeDescriptionType EdgeDescriptionType;

typedef TupleStringHashFunction<TupleType, SourceIp> SourceHash;
typedef TupleStringHashFunction<TupleType, DestIp> TargetHash;
typedef ZeroMQPushPull<EdgeType, Tuplizer, 
                       SourceHash, TargetHash> PartitionType;


BOOST_AUTO_TEST_CASE( test_triangles_exact )
{
  /// In this test, we create two threads that generate random netflows.
  /// Each thread has a ZeroMQPushPull object that consumes the netflows,
  /// and then feeds that to a GraphStore object.  We look for triangles.
  /// Since the ips are completely random, the chance that a triangle is
  /// is formed is very low.  We specify the number of triangles we want to
  /// find, and then manually create them, interspersed though the generation
  /// of the random netflows.

  // Setting up featureMap
  auto featureMap = std::make_shared<FeatureMap>(1000);

  // Setting up random generators
  AbstractVastNetflowGenerator *generator0 = new RandomGenerator();
  AbstractVastNetflowGenerator *generator1 = new RandomGenerator();

  // Setting up ZeroMQPushPull objects
  size_t queueLength = 1;
  size_t numNodes = 2;
  size_t nodeId0 = 0;
  size_t nodeId1 = 1;
  size_t hwm = 2000;

  size_t numTuples = 8000;

  // Sometimes it doesn't catch the triangles at the end because things 
  // terminate too quickly.  This adds a little buffer at the end where
  // no triangles occur.
  size_t numExtra = 1;

  size_t startingPort = 10000;
  size_t timeout = 2000;

  std::vector<std::string> hostnames;
  hostnames.push_back("localhost");
  hostnames.push_back("localhost");

  PartitionType* pushPull0 = new PartitionType(queueLength,
                                    numNodes, nodeId0,
                                    hostnames, 
                                    startingPort, timeout, true,
                                    hwm);

  PartitionType* pushPull1 = new PartitionType(queueLength,
                                    numNodes, nodeId1,
                                    hostnames, 
                                    startingPort, timeout, true,
                                    hwm);

  // To make things simpler, make sure numTriangles evenly divides numTuples
  // TODO This fails (on my mac) when numTriangles = 400.  A message 
  // deterministically fails to send for triangle 2_0, namely,
  // nodey_2_0,nodez_2_0.  Not sure why zeromq refuses to send that message.  There
  // are also two other messages that fail to send in PushPull object for 
  // EdgeRequestMap.  With numTriangles = 400, no messages fail to send.
  // Doesn't make much sense.  Maybe should try out different message broker.
  size_t numTriangles = 400;
  size_t modValue = numTuples / numTriangles;

  // Setting up GraphStore objects
  size_t graphCapacity = 1000; //For csc and csr
  size_t tableCapacity = 1000; //For SubgraphQueryResultMap intermediate results
  size_t resultsCapacity = 1000; //For final results
  double timeWindow = 100;

  startingPort = 10002;
  size_t numPushSockets = 1;
  size_t numPullThreads = 1;

  auto graphStore0 = std::make_shared<GraphStoreType>(
                          numNodes, nodeId0,
                          hostnames, startingPort, 
                          hwm, graphCapacity,
                          tableCapacity, resultsCapacity, 
                          numPushSockets, numPullThreads, timeout,
                          timeWindow, featureMap, MAX_NUM_FUTURES, true);

  auto graphStore1 = std::make_shared<GraphStoreType>(
                          numNodes, nodeId1,
                          hostnames, startingPort, 
                          hwm, graphCapacity,
                          tableCapacity, resultsCapacity, 
                          numPushSockets, numPullThreads, timeout,
                          timeWindow, featureMap, MAX_NUM_FUTURES, true);


  // Set up GraphStore objects to get input from ZeroMQPushPull objects
  pushPull0->registerConsumer(graphStore0);
  pushPull1->registerConsumer(graphStore1);

  // Set up the triangle query
  EdgeFunction starttimeFunction = EdgeFunction::StartTime;
  EdgeFunction endtimeFunction = EdgeFunction::EndTime;
  EdgeOperator equal_edge_operator = EdgeOperator::Assignment;
  EdgeOperator greater_edge_operator = EdgeOperator::GreaterThan;

  std::string e0 = "e0";
  std::string e1 = "e1";
  std::string e2 = "e2";
  std::string nodex = "nodex";
  std::string nodey = "nodey";
  std::string nodez = "nodez";

  EdgeExpression x2y(nodex, e0, nodey);
  EdgeExpression y2z(nodey, e1, nodez);
  EdgeExpression z2x(nodez, e2, nodex);
  TimeEdgeExpression startE0Both(starttimeFunction, e0, equal_edge_operator, 0);
  TimeEdgeExpression startE1Both(starttimeFunction,e1,greater_edge_operator, 0);
  TimeEdgeExpression startE2Both(starttimeFunction,e2,greater_edge_operator, 0);

  auto query = std::make_shared<SubgraphQueryType>(featureMap);
  query->addExpression(x2y);
  query->addExpression(y2z);
  query->addExpression(z2x);
  query->addExpression(startE0Both);
  query->addExpression(startE1Both);
  query->addExpression(startE2Both);
  query->finalize();

  graphStore0->registerQuery(query);
  graphStore1->registerQuery(query);

  // Checking that the query laid out how we expect
  EdgeDescriptionType const& edge0 = query->getEdgeDescription(0);
  EdgeDescriptionType const& edge1 = query->getEdgeDescription(1);
  EdgeDescriptionType const& edge2 = query->getEdgeDescription(2);

  BOOST_CHECK_EQUAL(edge0.source, nodex);
  BOOST_CHECK_EQUAL(edge1.source, nodey);
  BOOST_CHECK_EQUAL(edge2.source, nodez);

  double time = 0.0;
  double increment = 0.01;

  // The lambda function
  auto generateFunction = [numTuples, numTriangles, modValue, numExtra,
                           &time, increment](
                             PartitionType* pushPull,
                             AbstractVastNetflowGenerator* generator,
                             size_t nodeId)
  {

     
    auto starttime = std::chrono::high_resolution_clock::now();

    size_t totalTuples = 0;
    size_t triangleCounter = 0;
    Tuplizer tuplizer;
    for(size_t i = 0; i < numTuples; i++) {
        
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
      

      if (i % modValue == 0) {
        std::string nodex = "nodex_" + 
          boost::lexical_cast<std::string>(triangleCounter) +
          "_" + boost::lexical_cast<std::string>(nodeId);
        std::string nodey = "nodey_" + 
          boost::lexical_cast<std::string>(triangleCounter) +
          "_" + boost::lexical_cast<std::string>(nodeId);
        std::string nodez = "nodez_" + 
          boost::lexical_cast<std::string>(triangleCounter) +
          "_" + boost::lexical_cast<std::string>(nodeId);

        EdgeType edge0 = tuplizer(totalTuples++, str);
        std::get<SourceIp>(edge0.tuple) = nodex; 
        std::get<DestIp>(edge0.tuple) = nodey;

        // We create a triangle by adding two more edges.
        std::string str1 = generator->generate(time);
        time += increment;
        std::string str2 = generator->generate(time);
        time += increment;
        EdgeType edge1 = tuplizer(totalTuples++, str1);
        EdgeType edge2 = tuplizer(totalTuples++, str2);
        std::get<SourceIp>(edge1.tuple) = nodey;
        std::get<DestIp>(edge1.tuple) = nodez;
        std::get<SourceIp>(edge2.tuple) = nodez;
        std::get<DestIp>(edge2.tuple) = nodex;

        std::string str0 = edge0.toString();
        str1 = edge1.toString();
        str2 = edge2.toString();

        DEBUG_PRINT("Creating triangle: str %s str1 %s str2 %s\n", 
          str0.c_str(), str1.c_str(), str2.c_str());
        pushPull->consume(edge0);
        pushPull->consume(edge1);
        pushPull->consume(edge2);

        triangleCounter++;
      } else {
        EdgeType edge = tuplizer(totalTuples++, str);
        pushPull->consume(edge);
      }

    }

    auto endtime = std::chrono::high_resolution_clock::now();
    duration<double> diffTimeRegTuples = duration_cast<duration<double>>(
      endtime - starttime);
    printf("Time for node %lu for %lu tuples (time increament %f): %f\n", 
            nodeId, totalTuples, increment, diffTimeRegTuples.count());

    for(size_t i = 0; i < numExtra; i++) {
      std::string str = generator->generate(time);
      EdgeType edge = tuplizer(totalTuples++, str);
      time += increment;
      pushPull->consume(edge);
    }
    pushPull->terminate();
    

  };

  std::thread thread0(generateFunction, pushPull0, generator0, 0);
  std::thread thread1(generateFunction, pushPull1, generator1, 1);

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

  // Both threads creates numTriangles triangles.
  BOOST_CHECK_EQUAL(2 * numTriangles, totalResults); 

  printf("deleting pushpull0\n");
  delete pushPull0;
  printf("deleting pushpull1\n");
  delete pushPull1;
  printf("deleting generator0\n");
  delete generator0;
  printf("deleting generator1\n");
  delete generator1;


  printf("exiting\n");
}

/*
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
  AbstractVastNetflowGenerator *generator0 = new RandomPoolGenerator(numVertices);
  AbstractVastNetflowGenerator *generator1 = new RandomPoolGenerator(numVertices);
    

  // Setting up ZeroMQPushPull objects
  size_t queueLength = 1;
  size_t numNodes = 2;
  size_t nodeId0 = 0;
  size_t nodeId1 = 1;
  size_t hwm = 1000;

  size_t numTuples = 10000;

  // Sometimes it doesn't catch the triangles at the end because things 
  // terminate too quickly.  This adds a little buffer at the end where
  // no triangles occur.
  //size_t numExtra = 100;

  size_t startingPort = 10000;
  size_t timeout = 1000;

  std::vector<std::string> hostnames;
  hostnames.push_back("localhost");
  hostnames.push_back("localhost");

  PartitionType* pushPull0 = new PartitionType(queueLength,
                                    numNodes, nodeId0,
                                    hostnames, 
                                    startingPort, timeout, true,
                                    hwm);

  PartitionType* pushPull1 = new PartitionType(queueLength,
                                    numNodes, nodeId1,
                                    hostnames, 
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

  auto featureMap = std::make_shared<FeatureMap>(1000);

  auto graphStore0 = std::make_shared<GraphStoreType>(
                          numNodes, nodeId0,
                          hostnames, startingPort,
                          hwm, graphCapacity,
                          tableCapacity, resultsCapacity,
                          numPushSockets, numPullThreads, timeout,
                          timeWindow, featureMap, MAX_NUM_FUTURES, true);

  auto graphStore1 = std::make_shared<GraphStoreType>(
                          numNodes, nodeId1,
                          hostnames, startingPort,
                          hwm, graphCapacity,
                          tableCapacity, resultsCapacity, 
                          numPushSockets, numPullThreads, timeout,
                          timeWindow, featureMap, MAX_NUM_FUTURES, true );


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


  auto query = std::make_shared<SubgraphQueryType>(featureMap);
  query->addExpression(x2y);
  query->addExpression(y2z);
  query->addExpression(z2x);
  query->addExpression(startE0First);
  query->addExpression(startE1First);
  query->addExpression(startE2First);
  query->addExpression(startE0Second);
  query->addExpression(startE1Second);
  query->addExpression(startE2Second);
  query->finalize();

  graphStore0->registerQuery(query);
  graphStore1->registerQuery(query);

  // Checking that the query laid out how we expect
  EdgeDescriptionType const& edge0 = query->getEdgeDescription(0);
  EdgeDescriptionType const& edge1 = query->getEdgeDescription(1);
  EdgeDescriptionType const& edge2 = query->getEdgeDescription(2);

  BOOST_CHECK_EQUAL(edge0.source, nodex);
  BOOST_CHECK_EQUAL(edge1.source, nodey);
  BOOST_CHECK_EQUAL(edge2.source, nodez);

  double time = 0.0;
  double increment = 0.01;

  std::vector<VastNetflow> netflowList;
  std::mutex lock;

  // The lambda function
  auto generateFunction = [numTuples, &time, increment,
                           &netflowList, &lock]
                           (
                             PartitionType* pushPull,
                             AbstractVastNetflowGenerator* generator,
                             std::shared_ptr<GraphStoreType> graphStore,
                             size_t nodeId)
  {
    Tuplizer tuplizer;  
    auto starttime = std::chrono::high_resolution_clock::now();
    AbstractVastNetflowGenerator *otherGenerator = 
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
      EdgeType edge = tuplizer(i, str);
      time += increment;
      pushPull->consume(edge);
      lock.lock();
      netflowList.push_back(edge.tuple);
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
        printf("behind by %f\n", i * increment + numTuples * increment - 
               diff.count());
      }

     
      std::string str = otherGenerator->generate(time);
      EdgeType edge = tuplizer(i, str);
      time += increment;
      pushPull->consume(edge);
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
    VastNetflow n0 = result.getResultTuple(0).tuple; 
    VastNetflow n1 = result.getResultTuple(1).tuple; 
    VastNetflow n2 = result.getResultTuple(2).tuple; 
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
    VastNetflow n0 = result.getResultTuple(0).tuple; 
    VastNetflow n1 = result.getResultTuple(1).tuple; 
    VastNetflow n2 = result.getResultTuple(2).tuple; 
    double starttime0 = std::get<TimeSeconds>(n0);
    double starttime1 = std::get<TimeSeconds>(n1);
    double starttime2 = std::get<TimeSeconds>(n2);
    BOOST_CHECK(starttime0 <= starttime1);
    BOOST_CHECK(starttime1 <= starttime2);
    BOOST_CHECK(starttime2 - starttime0 < queryTimeWindow);
  }

  size_t calculatedNumTriangles = 
    sam::numTriangles<VastNetflow, SourceIp, DestIp, TimeSeconds,
                      DurationSeconds>(netflowList, queryTimeWindow);

  BOOST_CHECK_EQUAL(calculatedNumTriangles, totalResults);

  delete pushPull0;
  delete pushPull1;
  delete generator0;
  delete generator1;
}*/

