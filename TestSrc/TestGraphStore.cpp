#define BOOST_TEST_MAIN TestGraphStore

#define DEBUG

#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include <sam/tuples/Edge.hpp>
#include <sam/tuples/VastNetflow.hpp>
#include <sam/tuples/Tuplizer.hpp>
#include <sam/GraphStore.hpp>
#include <sam/tuples/VastNetflowGenerators.hpp>
#include <zmq.hpp>

using namespace sam;
using namespace sam::vast_netflow;
using namespace std::chrono;

typedef VastNetflow TupleType;
typedef EmptyLabel LabelType;
typedef Edge<size_t, LabelType, TupleType> EdgeType;
typedef TuplizerFunction<EdgeType, MakeVastNetflow> Tuplizer;
typedef GraphStore<EdgeType, Tuplizer, 
                   SourceIp, DestIp, 
                   TimeSeconds, DurationSeconds, 
                   StringHashFunction, StringHashFunction, 
                   StringEqualityFunction, StringEqualityFunction>
        GraphStoreType;

typedef EdgeDescription<TupleType, TimeSeconds, DurationSeconds>
        EdgeDescriptionType;

typedef GraphStoreType::QueryType QueryType;


BOOST_AUTO_TEST_CASE( test_graph_store )
{
  /// In this test we create a graphstore on two nodes (both local addresses).
  ///
  size_t numNodes = 2;
  size_t nodeId0 = 0;
  size_t nodeId1 = 1;
  size_t hwm = 1000;
  size_t graphCapacity = 1000; //For csc and csr
  size_t tableCapacity = 1000; //For SubgraphQueryResultMap intermediate results
  size_t resultsCapacity = 1000; //For final results
  double timeWindow = 100;
  size_t startingPort = 10000;

  std::vector<std::string> hostnames;
  hostnames.push_back("localhost");
  hostnames.push_back("localhost");

  int n = 1000;

  size_t numThreads = 1;

  size_t numPushSockets = 1;
  size_t numPullThreads = 1;
  size_t timeout = 2000;
  size_t maxFutures = 1;
  bool local = true;
  auto featureMap = std::make_shared<FeatureMap>(1000);

  std::cout << "blah1" << std::endl;
  GraphStoreType* graphStore0 = new GraphStoreType(
                        numNodes, nodeId0, 
                        hostnames, startingPort,
                        hwm, graphCapacity, 
                        tableCapacity, resultsCapacity, 
                        numPushSockets, numPullThreads, timeout, 
                        timeWindow, featureMap, maxFutures, local); 
  std::cout << "blah2" << std::endl;

  // One thread runs this.
  auto graph_function0 = [graphStore0, n]()
  {
    Tuplizer tuplizer;
    AbstractVastNetflowGenerator *generator0 = 
      new UniformDestPort("192.168.0.0", 1);
    
    for (int i = 0; i < n; i++) {
      std::cout << "testgraphstore i " << i << std::endl;
      std::string str = generator0->generate();
      std::cout << "testgraphstore str " << str << std::endl;
      EdgeType edge = tuplizer(i, str);
      graphStore0->consume(edge);
    }
    graphStore0->terminate();

    
    delete generator0;
  };

  /*
  GraphStoreType* graphStore1 = new GraphStoreType(
                        numNodes, nodeId1, 
                        hostnames, startingPort + 4,
                        hwm, graphCapacity, 
                        tableCapacity, resultsCapacity, 
                        numPushSockets, numPullThreads, timeout, 
                        timeWindow, featureMap, 1, true); 

  // Another thread runs this.
  auto graph_function1 = [graphStore1, n]()
                          
  {
    AbstractVastNetflowGenerator *generator1 = 
      new UniformDestPort("192.168.0.1", 1);
   
    Tuplizer tuplizer; 
    //for (int i = 0; i < n; i++) {
    //  std::string str = generator1->generate();
    //  TupleLabel tupleLabel = tuplizer(str);
    //  graphStore1->consume(i, tupleLabel);
    //}
    graphStore1->terminate();

    
    delete generator1;
  };*/

  std::thread thread0(graph_function0);
  //std::thread thread1(graph_function1);

  thread0.join();
  //thread1.join();


  // There is no query that forces communication, so the number of received
  // tuples over zeromq should be zero.
  BOOST_CHECK_EQUAL(graphStore0->getTotalEdgePulls(), 0);
  //BOOST_CHECK_EQUAL(graphStore1->getTotalEdgePulls(), 0);

  delete graphStore0;
  //delete graphStore1;
}


/*
struct SingleNodeFixture  {

  size_t numNodes = 1;
  size_t nodeId0 = 0;
  size_t hwm = 1000;
  size_t graphCapacity = 1000; //For csc and csr
  size_t tableCapacity = 1000; //For SubgraphQueryResultMap results
  size_t resultsCapacity = 1000; //For final results
  double timeWindow = 100;

  std::vector<std::string> hostnames;
  size_t startingPort = 10000;

  size_t numPushSockets = 1;
  size_t numPullThreads = 1;
  size_t timeout = 1000;

  EdgeFunction starttimeFunction = EdgeFunction::StartTime;
  EdgeFunction endtimeFunction = EdgeFunction::EndTime;
  EdgeOperator equal_edge_operator = EdgeOperator::Assignment;
  EdgeOperator greater_edge_operator = EdgeOperator::GreaterThan;
  std::string e1 = "e1";
  std::string e2 = "e2";
  std::string nodex = "nodex";
  std::string nodey = "nodey";
  std::string nodez = "nodez";

  EdgeExpression* y2x;
  EdgeExpression* z2x;
  TimeEdgeExpression* startY2Xboth;
  TimeEdgeExpression* startZ2Xbeg;

  AbstractVastNetflowGenerator* generator;

  GraphStoreType* graphStore0;
  std::shared_ptr<FeatureMap> featureMap;

  SingleNodeFixture () {
    y2x = new EdgeExpression(nodey, e1, nodex);
    z2x = new EdgeExpression(nodez, e2, nodex);
    startY2Xboth = new TimeEdgeExpression(starttimeFunction,
                                          e1, equal_edge_operator, 0);
    startZ2Xbeg = new TimeEdgeExpression(starttimeFunction,
                                         e2, greater_edge_operator, 0); 
    generator = new UniformDestPort("192.168.0.2", 1);

    size_t numThreads = 1;

    hostnames.push_back("localhost");
    featureMap = std::make_shared<FeatureMap>(1000);

    graphStore0 = new GraphStoreType(numNodes, nodeId0, 
                        hostnames, startingPort,
                        hwm, graphCapacity, 
                        tableCapacity, resultsCapacity, 
                        numPushSockets, numPullThreads, timeout,
                        timeWindow, featureMap, 1, true); 
  }

  ~SingleNodeFixture() {
    delete y2x;
    delete z2x;
    delete startY2Xboth;
    delete startZ2Xbeg;
    delete generator;
    delete graphStore0;
  }
};

///
/// In this test the query is simply an edge such that every edge
/// matches.
///
BOOST_FIXTURE_TEST_CASE( test_single_edge_match, SingleNodeFixture )
{
  auto query = std::make_shared<QueryType>(featureMap);

  query->addExpression(*startY2Xboth);
  query->addExpression(*y2x);

  // Should complain that the query hasn't been finalized
  BOOST_CHECK_THROW(graphStore0->registerQuery(query), GraphStoreException);

  query->finalize();

  graphStore0->registerQuery(query);
  
  Tuplizer tuplizer;
  size_t n = 1000;
  for(size_t i = 0; i < n; i++) 
  {
    std::string str = generator->generate();
    TupleLabel tupleLabel = tuplizer(str);
    graphStore0->consume(i, tupleLabel);
  }

  graphStore0->terminate();

  std::this_thread::sleep_for(
      std::chrono::milliseconds(1000));
  BOOST_CHECK_EQUAL(graphStore0->getNumResults(), n);
}

BOOST_FIXTURE_TEST_CASE( test_double_terminate, SingleNodeFixture )
{
  graphStore0->terminate();
  graphStore0->terminate();
}



///
/// In this test the query is simply an edge such but the time constraints
/// make so that nothing matches.
///
BOOST_FIXTURE_TEST_CASE( test_single_edge_no_match, SingleNodeFixture )
{
  auto query = std::make_shared<QueryType>(featureMap);

  TimeEdgeExpression endTimeExpressionE1(endtimeFunction, e1, 
                                         equal_edge_operator, 0);
                                         
 
  query->addExpression(*startY2Xboth);
  query->addExpression(endTimeExpressionE1);
  query->addExpression(*y2x);
  query->finalize();

  graphStore0->registerQuery(query);

  Tuplizer tuplizer;
  size_t n = 10000;
  for(size_t i = 0; i < n; i++) 
  {
    std::string str = generator->generate();
    TupleLabel tupleLabel = tuplizer(str);
    graphStore0->consume(i, tupleLabel);
  }

  BOOST_CHECK_EQUAL(graphStore0->getNumResults(), 0);
}


///
/// In this test the query is two connected edges.
///
BOOST_FIXTURE_TEST_CASE( test_double_edge_match, SingleNodeFixture )
{
  auto query = std::make_shared<QueryType>(featureMap);

  TimeEdgeExpression startTimeExpressionE2(starttimeFunction, e2,
                                           greater_edge_operator, 0);
 
  query->addExpression(*startY2Xboth);
  query->addExpression(*startZ2Xbeg);
  query->addExpression(*y2x);
  query->addExpression(*z2x);
  query->finalize();

  graphStore0->registerQuery(query);

  size_t numExtra = 100;
  //size_t numExtra = 1;

  double rate = 100;
  double increment = 1 / rate;
  double time = 0.0;

  auto t1 = std::chrono::high_resolution_clock::now();
 
  Tuplizer tuplizer; 
  size_t n = 100;
  size_t totalNetflows = 0;
  for(size_t i = 0; i < n; i++) 
  {
    printf("i %lu\n", i);
    auto currenttime = std::chrono::high_resolution_clock::now();
    duration<double> diff = 
      duration_cast<duration<double>>(currenttime - t1);
    if (diff.count() < totalNetflows * increment) 
    {
      size_t numMilliseconds = (totalNetflows*increment - diff.count())*1000;
      std::this_thread::sleep_for(
        std::chrono::milliseconds(numMilliseconds));
    } 

    std::string str = generator->generate(time);
    TupleLabel tupleLabel = tuplizer(str);
    graphStore0->consume(i, tupleLabel);
    time += increment;
    totalNetflows++;
  }

  RandomGenerator randomGenerator;
  for(size_t i = 0; i < numExtra; i++) 
  {
    printf("extra %lu\n", i);
    auto currenttime = std::chrono::high_resolution_clock::now();
    duration<double> diff = duration_cast<duration<double>>(currenttime - t1);
    if (diff.count() < totalNetflows * increment) 
    {
      size_t numMilliseconds = 
        (totalNetflows * increment - diff.count()) * 1000;
      std::this_thread::sleep_for(
        std::chrono::milliseconds(numMilliseconds));
    }

    std::string str = randomGenerator.generate();
    TupleLabel tupleLabel = tuplizer(str);
    graphStore0->consume(totalNetflows++, tupleLabel);
  }

  graphStore0->terminate();

  printf("numResults %zu expected %zu\n", graphStore0->getNumResults(),
         (n-1)*(n)/2);
  BOOST_CHECK_EQUAL(graphStore0->getNumResults(), (n-1)*(n)/2);
 
}

///
/// This tests where two of the edges in the triangle have the same 
/// time.  We are assuming strictly increasing time for the edges, but
/// TODO that should be specifiable.
BOOST_FIXTURE_TEST_CASE( test_triangle_same_time, SingleNodeFixture )
{
  // Set up the triangle query
  double queryTimeWindow = 10;
  //SubgraphQueryType query;
  
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
  TimeEdgeExpression endE0Second(endtimeFunction, e0,
                                   less_edge_operator,queryTimeWindow);
  TimeEdgeExpression endE1Second(endtimeFunction,e1,
                                   less_edge_operator,queryTimeWindow);
  TimeEdgeExpression endE2Second(endtimeFunction,e2,
                                   less_edge_operator, queryTimeWindow);

  auto query = std::make_shared<QueryType>(featureMap);
  query->addExpression(x2y);
  query->addExpression(y2z);
  query->addExpression(z2x);
  query->addExpression(startE0First);
  query->addExpression(startE1First);
  query->addExpression(startE2First);
  query->addExpression(startE0Second);
  query->addExpression(startE1Second);
  query->addExpression(startE2Second);
  query->addExpression(endE0Second);
  query->addExpression(endE1Second);
  query->addExpression(endE2Second);
  query->finalize();

  graphStore0->registerQuery(query);

  std::string s1 = "0.47000000000000025,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node1,node2,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s2 = "0.52000000000000024,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node2,node3,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s3 = "0.52000000000000024,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node3,node1,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s4 = "0.47000000000000025,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node4,node5,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s5 = "0.47000000000000025,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node5,node6,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s6 = "0.52000000000000024,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node6,node4,51482,40020,1,1,1,1,1,1,1,1,1,1";


  Tuplizer tuplizer;
  TupleLabel n1 = tuplizer(s1);
  TupleLabel n2 = tuplizer(s2);
  TupleLabel n3 = tuplizer(s3);
  TupleLabel n4 = tuplizer(s4);
  TupleLabel n5 = tuplizer(s5);
  TupleLabel n6 = tuplizer(s6);
  graphStore0->consume(0, n1);
  graphStore0->consume(1, n2);
  graphStore0->consume(2, n3);
  graphStore0->consume(0, n4);
  graphStore0->consume(1, n5);
  graphStore0->consume(2, n6);

  graphStore0->terminate();

  BOOST_CHECK_EQUAL(graphStore0->getNumResults(), 0);
}
*/

