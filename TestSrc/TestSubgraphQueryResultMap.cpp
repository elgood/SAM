#define BOOST_TEST_MAIN TestSubgraphQueryResultMap

#include <boost/test/unit_test.hpp>
#include "Util.hpp"
#include "SubgraphQuery.hpp"
#include "SubgraphQueryResult.hpp"
#include "SubgraphQueryResultMap.hpp"
#include "Netflow.hpp"
#include "NetflowGenerators.hpp"

using namespace sam;

typedef SubgraphQueryResultMap<Netflow, SourceIp, DestIp,
  TimeSeconds, DurationSeconds, StringHashFunction, StringHashFunction,
  StringEqualityFunction, StringEqualityFunction> MapType;

typedef MapType::EdgeRequestType EdgeRequestType;

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

  std::shared_ptr<AbstractNetflowGenerator> generator;

  F () {
    y2x = std::make_shared<EdgeExpression>(nodey, e1, nodex);
    z2x = std::make_shared<EdgeExpression>(nodez, e2, nodex);
    startY2Xboth = std::make_shared<TimeEdgeExpression>(starttimeFunction,
                                                  e1, equal_edge_operator, 0);
    startZ2Xbeg = std::make_shared<TimeEdgeExpression>(starttimeFunction,
                                                 e2, greater_edge_operator, 0); 
    generator = std::make_shared<UniformDestPort>("192.168.0.2", 1);
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
  MapType map(numNodes, nodeId, tableCapacity, resultCapacity); 

  SubgraphQuery<Netflow, TimeSeconds, DurationSeconds> query;

  query.addExpression(*startY2Xboth);
  query.addExpression(*y2x);
  query.finalize();

  std::list<EdgeRequestType> edgeRequests;
  size_t n = 10000;
  for(size_t i = 0; i < n; i++) 
  {
    std::string str = generator->generate();
    Netflow netflow = makeNetflow(0, str);
    SubgraphQueryResult<Netflow, SourceIp, DestIp, TimeSeconds,
                        DurationSeconds> result(&query, netflow);
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
  MapType map(numNodes, nodeId, tableCapacity, resultCapacity); 

  SubgraphQuery<Netflow, TimeSeconds, DurationSeconds> query;


  TimeEdgeExpression endTimeExpressionE1(endtimeFunction, e1, 
                                         equal_edge_operator, 0);
                                         
 
  query.addExpression(*startY2Xboth);
  query.addExpression(endTimeExpressionE1);
  query.addExpression(*y2x);
  query.finalize();

  std::list<EdgeRequestType> edgeRequests;
  size_t n = 10000;
  for(size_t i = 0; i < n; i++) 
  {
    std::string str = generator->generate();
    Netflow netflow = makeNetflow(0, str);
    double startTime = std::get<TimeSeconds>(netflow);
    if (query.satisfies(netflow, 0, startTime)) { 
      SubgraphQueryResult<Netflow, SourceIp, DestIp, TimeSeconds,
                          DurationSeconds> result(&query, netflow);
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
  MapType map(numNodes, nodeId, tableCapacity, resultCapacity); 

  SubgraphQuery<Netflow, TimeSeconds, DurationSeconds> query;


  TimeEdgeExpression startTimeExpressionE2(starttimeFunction, e2,
                                           greater_edge_operator, 0);
 

  query.addExpression(*startY2Xboth);
  query.addExpression(*startZ2Xbeg);
  query.addExpression(*y2x);
  query.addExpression(*z2x);
  query.finalize();

  std::list<EdgeRequestType> edgeRequests;
  size_t n = 10;
  for(size_t i = 0; i < n; i++) 
  {
    std::string str = generator->generate();
    Netflow netflow = makeNetflow(i, str);
    SubgraphQueryResult<Netflow, SourceIp, DestIp, TimeSeconds,
                        DurationSeconds> result(&query, netflow);
    map.add(result, edgeRequests);
    map.process(netflow, edgeRequests);
  }
  BOOST_CHECK_EQUAL(edgeRequests.size(), 0);
  BOOST_CHECK_EQUAL(map.getNumResults(), (n-1)*(n)/2);
 
}


