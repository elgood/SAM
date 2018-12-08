#define BOOST_TEST_MAIN TestSubgraphQueryResult

#include <boost/test/unit_test.hpp>
#include "Util.hpp"
#include "SubgraphQueryResult.hpp"
#include "SubgraphQuery.hpp"
#include "FeatureMap.hpp"


using namespace sam;

struct F {
  std::string netflowString1 = "1,1,82756.384094,2013-04-10 08:32:36,"
                           "20130410083236.384094,17,UDP,target,"
                           "bait,29986,1900,0,0,0,133,0,1,0,1,0,0"; 
  Netflow netflow1 = makeNetflow(netflowString1);

  std::string netflowString2 = "1,1,82766.374094,2013-04-10 08:32:36,"
                           "20130410083236.384094,17,UDP,target,"
                           "controller,29986,1900,0,0,0,133,0,1,0,1,0,0"; 

  Netflow netflow2 = makeNetflow(netflowString2);

  std::string netflowString3 = "1,1,82867.384094,2013-04-10 08:32:36,"
                           "20130410083236.384094,17,UDP,target,"
                           "controller,29986,1900,0,0,0,133,0,1,0,1,0,0"; 

  Netflow netflow3 = makeNetflow(netflowString3);

  typedef SubgraphQueryResult<Netflow, SourceIp, DestIp, 
                              TimeSeconds, DurationSeconds> ResultType;

  std::shared_ptr<TimeEdgeExpression> endTimeExpressionE1;
  std::shared_ptr<EdgeExpression> targetE1Bait;

  std::shared_ptr<TimeEdgeExpression> startTimeExpressionE2_begin;
  std::shared_ptr<TimeEdgeExpression> startTimeExpressionE2_end;
  std::shared_ptr<EdgeExpression> targetE2Controller;
  
  std::shared_ptr<FeatureMap> featureMap;

  F () {
    endTimeExpressionE1 = std::make_shared<TimeEdgeExpression>(
      EdgeFunction::StartTime, "e1", EdgeOperator::Assignment, 0);
    targetE1Bait = std::make_shared<EdgeExpression>("target1", "e1", "bait");
    startTimeExpressionE2_begin = std::make_shared<TimeEdgeExpression>(
      EdgeFunction::StartTime, "e2", EdgeOperator::GreaterThan, 0);
    startTimeExpressionE2_end = std::make_shared<TimeEdgeExpression>(
      EdgeFunction::StartTime, "e2", EdgeOperator::LessThan, 10);
    targetE2Controller = std::make_shared<EdgeExpression>("target1", "e2",
      "controller");
    featureMap = std::make_shared<FeatureMap>(1000);
  }

  ~F() {

  }

};

BOOST_FIXTURE_TEST_CASE( test_check_one_edge, F )
{

  // Creates a subgraph query with just one edge and checks that
  // a query result after adding a netflow satisfies the query and 
  // completes it.
  SubgraphQuery<Netflow, TimeSeconds, DurationSeconds> query;
  query.addExpression(*endTimeExpressionE1);
  query.addExpression(*targetE1Bait);
 
  // Throws an error because query has not been finalized. 
  BOOST_CHECK_THROW( ResultType result(&query, netflow1, featureMap),
                   SubgraphQueryResultException);

  query.finalize();


  ResultType result(&query, netflow1, featureMap);

  BOOST_CHECK(result.complete());
}

BOOST_FIXTURE_TEST_CASE( test_check_two_edges, F )
{
  // target e1 bait
  // target e2 controller
  SubgraphQuery<Netflow, TimeSeconds, DurationSeconds> query;
  query.addExpression(*endTimeExpressionE1);
  query.addExpression(*targetE1Bait);
  query.addExpression(*startTimeExpressionE2_begin);
  query.addExpression(*startTimeExpressionE2_end);
  query.addExpression(*targetE2Controller);
  query.finalize();

  ResultType result(&query, netflow1, featureMap);

  BOOST_CHECK(!result.complete());

  result.addEdgeInPlace(netflow2);

  BOOST_CHECK(result.complete());

}

BOOST_FIXTURE_TEST_CASE( test_expired_edge, F )
{
  // Query:
  // target e1 bait;
  // starttime(e1) = 0;
  // target e2 controller;
  // starttime(e2) > 0;
  // starttime(e2) < 10;
  //
  // Tests giving an edge that doesn't fulfill time constraint.
  // Also checks that the query result is determined to be expired
  // when given a time that is past the max extent of the query.
  SubgraphQuery<Netflow, TimeSeconds, DurationSeconds> query;
  double maxOffset = 100.0;
  query.setMaxOffset(maxOffset);
  query.addExpression(*endTimeExpressionE1);
  query.addExpression(*targetE1Bait);
  query.addExpression(*startTimeExpressionE2_begin);
  query.addExpression(*startTimeExpressionE2_end);
  query.addExpression(*targetE2Controller);
  query.finalize();

  BOOST_CHECK_EQUAL(query.getMaxOffset(), maxOffset);

  ResultType result(&query, netflow1, featureMap);

  double netflow1Time = std::get<TimeSeconds>(netflow1);
  BOOST_CHECK_EQUAL(result.getExpireTime(), netflow1Time+maxOffset+10);

  BOOST_CHECK(!result.complete());

  result.addEdge(netflow3);

  BOOST_CHECK(!result.complete());

  double currentTime = std::get<TimeSeconds>(netflow3);
  BOOST_CHECK(result.isExpired(currentTime));

  BOOST_CHECK_EQUAL(result.getExpireTime(), netflow1Time + 
    startTimeExpressionE2_end->value + maxOffset);
}
