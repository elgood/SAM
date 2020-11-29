#define BOOST_TEST_MAIN TestSubgraphQueryResultMap

#include <boost/test/unit_test.hpp>
#include <sam/Util.hpp>
#include <sam/SubgraphQuery.hpp>
#include <sam/SubgraphQueryResult.hpp>
#include <sam/SubgraphQueryResultMap.hpp>
#include <sam/tuples/Edge.hpp>
#include <sam/tuples/Tuplizer.hpp>
#include <sam/tuples/VastNetflow.hpp>
#include <sam/tuples/VastNetflowGenerators.hpp>


using namespace sam;
using namespace sam::vast_netflow;
using namespace std::chrono;

typedef VastNetflow TupleType;
typedef EmptyLabel LabelType;
typedef Edge<size_t, LabelType, TupleType> EdgeType;
typedef TuplizerFunction<EdgeType, MakeVastNetflow> Tuplizer;
typedef SubgraphQueryResultMap<EdgeType, SourceIp, DestIp,
  TimeSeconds, DurationSeconds, StringHashFunction, StringHashFunction,
  StringEqualityFunction, StringEqualityFunction> MapType;
typedef SubgraphQuery<TupleType, SourceIp, DestIp, 
  TimeSeconds, DurationSeconds> QueryType;
  

typedef MapType::QueryResultType QueryResultType;

typedef MapType::EdgeRequestType EdgeRequestType;

typedef MapType::CscType CscType;
typedef MapType::CsrType CsrType;

struct F {
  EdgeFunction starttimeFunction = EdgeFunction::StartTime;
  EdgeFunction endtimeFunction = EdgeFunction::EndTime;
  EdgeOperator equal_edge_operator = EdgeOperator::Assignment;
  EdgeOperator greater_edge_operator = EdgeOperator::GreaterThan;
  std::string e1 = "e1";
  std::string e2 = "e2";
  std::string nodex = "nodex";
  std::string nodey = "nodey";
  std::string nodez = "nodez";

  std::shared_ptr<EdgeExpression> y2x;
  std::shared_ptr<EdgeExpression> z2x;
  std::shared_ptr<TimeEdgeExpression> startY2Xboth;
  std::shared_ptr<TimeEdgeExpression> startZ2Xbeg;

  std::shared_ptr<AbstractVastNetflowGenerator> generator;

  size_t capacity = 1000;
  double window = 100;
  CsrType* csr;
  CscType* csc;

  std::shared_ptr<FeatureMap> featureMap;

  F () {
    y2x = std::make_shared<EdgeExpression>(nodey, e1, nodex);
    z2x = std::make_shared<EdgeExpression>(nodez, e2, nodex);
    startY2Xboth = std::make_shared<TimeEdgeExpression>(starttimeFunction,
                                                  e1, equal_edge_operator, 0);
    startZ2Xbeg = std::make_shared<TimeEdgeExpression>(starttimeFunction,
                                                 e2, greater_edge_operator, 0); 
    generator = std::make_shared<UniformDestPort>("192.168.0.2", 1);
    csr = new CsrType(capacity, window);
    csc = new CscType(capacity, window);
    featureMap = std::make_shared<FeatureMap>(1000);
  }

  ~F () {
    delete csr;
    delete csc;
  }

};

///
/// In this test the query is simply an edge such that every edge
/// matches.
///
BOOST_FIXTURE_TEST_CASE( test_single_edge_match, F )
{
  size_t tableCapacity = 1000;
  size_t resultCapacity = 1000;
  size_t numNodes = 1;
  size_t nodeId = 0;
  MapType map(numNodes, nodeId, tableCapacity, resultCapacity, *csr, *csc); 

  auto query = std::make_shared<QueryType>(featureMap);

  query->addExpression(*startY2Xboth);
  query->addExpression(*y2x);
  query->finalize();

  Tuplizer tuplizer;
  std::list<EdgeRequestType> edgeRequests;
  size_t n = 10000;
  for(size_t i = 0; i < n; i++) 
  {
    std::string str = generator->generate();
    EdgeType edge = tuplizer(i, str);
    QueryResultType result(query, edge);
    map.add(result, edgeRequests);
  }

  BOOST_CHECK_EQUAL(edgeRequests.size(), 0);
  BOOST_CHECK_EQUAL(map.getNumResults(), n);
 
}

///
/// In this test the query is simply an edge such but the time constraints
/// make so that nothing matches.
///
BOOST_FIXTURE_TEST_CASE( test_single_edge_no_match, F )
{
  size_t tableCapacity = 1000;
  size_t resultCapacity = 1000;
  size_t numNodes = 1;
  size_t nodeId = 0;
  MapType map(numNodes, nodeId, tableCapacity, resultCapacity, *csr, *csc); 

  auto query = std::make_shared<QueryType>(featureMap);


  TimeEdgeExpression endTimeExpressionE1(endtimeFunction, e1, 
                                         equal_edge_operator, 0);
                                         
 
  query->addExpression(*startY2Xboth);
  query->addExpression(endTimeExpressionE1);
  query->addExpression(*y2x);
  query->finalize();

  std::list<EdgeRequestType> edgeRequests;

  Tuplizer tuplizer;
  size_t n = 10000;
  for(size_t i = 0; i < n; i++) 
  {
    std::string str = generator->generate();
    EdgeType edge = tuplizer(i, str);
    double startTime = std::get<TimeSeconds>(edge.tuple);
    if (query->satisfiesConstraints(0, edge.tuple, startTime))
    {
      QueryResultType result(query, edge);
      map.add(result, edgeRequests);
    }
  }

  BOOST_CHECK_EQUAL(edgeRequests.size(), 0);
  BOOST_CHECK_EQUAL(map.getNumResults(), 0);
 
}

///
/// In this test the query is two connected edges.
///
BOOST_FIXTURE_TEST_CASE( test_double_edge_match, F )
{
  size_t tableCapacity = 1000;
  size_t resultCapacity = 1000;
  size_t numNodes = 1;
  size_t nodeId = 0;
  MapType map(numNodes, nodeId, tableCapacity, resultCapacity, *csr, *csc); 

  auto query = std::make_shared<QueryType>(featureMap);


  TimeEdgeExpression startTimeExpressionE2(starttimeFunction, e2,
                                           greater_edge_operator, 0);
 

  query->addExpression(*startY2Xboth);
  query->addExpression(*startZ2Xbeg);
  query->addExpression(*y2x);
  query->addExpression(*z2x);
  query->finalize();

  std::list<EdgeRequestType> edgeRequests;
  size_t n = 50;

  double rate = 1000;
  double increment = 1 / rate;
  double time = 0.0;

  auto t1 = std::chrono::high_resolution_clock::now();

  Tuplizer tuplizer;
  for(size_t i = 0; i < n; i++) 
  {
    auto currenttime = std::chrono::high_resolution_clock::now();
    duration<double> diff = duration_cast<duration<double>>(currenttime - t1);
    if (diff.count() < i * increment) {
      size_t numMilliseconds = (i * increment - diff.count()) * 1000;
      std::this_thread::sleep_for(
        std::chrono::milliseconds(numMilliseconds));
    }


    std::string str = generator->generate(time);
    EdgeType edge = tuplizer(i, str);
    time += increment;
    QueryResultType result(query, edge);
                         
    map.add(result, edgeRequests);
    map.process(edge, edgeRequests);
  }
  BOOST_CHECK_EQUAL(edgeRequests.size(), 0);
  BOOST_CHECK_EQUAL(map.getNumResults(), (n-1)*(n)/2);
 
}

///
/// In this test the query is a->b, b->c, and c->d and the edges b->c and c->d
/// already exist in the graph. 
///
BOOST_FIXTURE_TEST_CASE( test_process_against_graph, F )
{
  std::string nodeA = "A";
  std::string nodeB = "B";
  std::string nodeC = "C";
  std::string nodeD = "D";

  std::string str1 = generator->generate(0.0);
  std::string str2 = generator->generate(0.1);
  std::string str3 = generator->generate(0.2);

  Tuplizer tuplizer;
  EdgeType netflow1 = tuplizer(1, str1);
  EdgeType netflow2 = tuplizer(2, str2);
  EdgeType netflow3 = tuplizer(3, str3);

  std::get<SourceIp>(netflow1.tuple) = nodeA; 
  std::get<DestIp>(netflow1.tuple) = nodeB; 
  std::get<SourceIp>(netflow2.tuple) = nodeB; 
  std::get<DestIp>(netflow2.tuple) = nodeC; 
  std::get<SourceIp>(netflow3.tuple) = nodeC; 
  std::get<DestIp>(netflow3.tuple) = nodeD; 

  csr->addEdge(netflow2);
  csr->addEdge(netflow3);
  csc->addEdge(netflow2);
  csc->addEdge(netflow3);

  auto query = std::make_shared<QueryType>(featureMap);

  EdgeExpression A2B("nodea", "e0", "nodeb");
  EdgeExpression B2C("nodeb", "e1", "nodec");
  EdgeExpression C2D("nodec", "e2", "noded");
  TimeEdgeExpression startTimeExpressionA2B(starttimeFunction, "e0",
                                           equal_edge_operator, 0);
  TimeEdgeExpression startTimeExpressionB2C(starttimeFunction, "e1",
                                           greater_edge_operator, 0);
  TimeEdgeExpression startTimeExpressionC2D(starttimeFunction, "e2",
                                           greater_edge_operator, 0);
 

  query->addExpression(A2B);
  query->addExpression(B2C);
  query->addExpression(C2D);
  query->addExpression(startTimeExpressionA2B);
  query->addExpression(startTimeExpressionB2C);
  query->addExpression(startTimeExpressionC2D);
  query->finalize();

  size_t id = 0;
  QueryResultType result(query, netflow1);

  std::list<EdgeRequestType> edgeRequests;
  
  size_t tableCapacity = 1000;
  size_t resultCapacity = 1000;
  size_t numNodes = 1;
  size_t nodeId = 0;
  MapType map(numNodes, nodeId, tableCapacity, resultCapacity, *csr, *csc); 

  map.add(result, edgeRequests);
  map.process(netflow1, edgeRequests);

  BOOST_CHECK_EQUAL(map.getNumResults(), 1);

}

