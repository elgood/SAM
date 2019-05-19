#define BOOST_TEST_MAIN TestSubgraphQueryResult

#include <boost/test/unit_test.hpp>
#include <sam/Util.hpp>
#include <sam/SubgraphQueryResult.hpp>
#include <sam/SubgraphQuery.hpp>
#include <sam/FeatureMap.hpp>

using namespace sam;

typedef SubgraphQuery<VastNetflow, SourceIp, DestIp, TimeSeconds, DurationSeconds>
  QueryType;

struct F {
  std::string netflowString1 = "1,1,156.0,2013-04-10 08:32:36,"
                           "20130410083236.384094,17,UDP,target,"
                           "bait,29986,1900,0,0,1.0,133,0,1,0,1,0,0"; 
  VastNetflow netflow1 = makeNetflow(netflowString1);

  std::string netflowString2 = "1,1,166.0,2013-04-10 08:32:36,"
                           "20130410083236.384094,17,UDP,target,"
                           "controller,29986,1900,0,0,1.0,133,0,1,0,1,0,0"; 

  VastNetflow netflow2 = makeNetflow(netflowString2);

  std::string netflowString3 = "1,1,267.01,2013-04-10 08:32:36,"
                           "20130410083236.384094,17,UDP,target,"
                           "controller,29986,1900,0,0,1.0,133,0,1,0,1,0,0"; 

  VastNetflow netflow3 = makeNetflow(netflowString3);

  typedef SubgraphQueryResult<VastNetflow, SourceIp, DestIp, 
                              TimeSeconds, DurationSeconds> ResultType;

  std::shared_ptr<TimeEdgeExpression> startTimeExpressionE1;
  std::shared_ptr<TimeEdgeExpression> endTimeExpressionE1;
  std::shared_ptr<EdgeExpression> targetE1Bait;

  std::shared_ptr<TimeEdgeExpression> startTimeExpressionE2_begin;
  std::shared_ptr<TimeEdgeExpression> startTimeExpressionE2_end;
  std::shared_ptr<EdgeExpression> targetE2Controller;
  
  std::shared_ptr<FeatureMap> featureMap;
  std::string bait = "bait";
  std::string controller = "controller";

  F () {
    startTimeExpressionE1 = std::make_shared<TimeEdgeExpression>(
      EdgeFunction::StartTime, "e1", EdgeOperator::Assignment, 0);
    endTimeExpressionE1 = std::make_shared<TimeEdgeExpression>(
      EdgeFunction::EndTime, "e1", EdgeOperator::Assignment, 0);
    targetE1Bait = std::make_shared<EdgeExpression>("target1", "e1", bait);
    startTimeExpressionE2_begin = std::make_shared<TimeEdgeExpression>(
      EdgeFunction::StartTime, "e2", EdgeOperator::GreaterThan, 0);
    startTimeExpressionE2_end = std::make_shared<TimeEdgeExpression>(
      EdgeFunction::StartTime, "e2", EdgeOperator::LessThan, 10);
    targetE2Controller = std::make_shared<EdgeExpression>("target1", "e2",
      controller);
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
  auto query = std::make_shared<QueryType>(featureMap);
  query->addExpression(*startTimeExpressionE1);
  query->addExpression(*targetE1Bait);
 
  // Throws an error because query has not been finalized. 
  BOOST_CHECK_THROW( ResultType result(query, netflow1),
                   SubgraphQueryResultException);

  query->finalize();

  ResultType result(query, netflow1);

  BOOST_CHECK(result.complete());
}

BOOST_FIXTURE_TEST_CASE( test_check_two_edges, F )
{
  // target e1 bait
  // target e2 controller
  auto query = std::make_shared<QueryType>(featureMap);
  query->addExpression(*startTimeExpressionE1);
  query->addExpression(*targetE1Bait);
  query->addExpression(*startTimeExpressionE2_begin);
  query->addExpression(*startTimeExpressionE2_end);
  query->addExpression(*targetE2Controller);
  query->finalize();

  ResultType result(query, netflow1);

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
  auto query = std::make_shared<QueryType>(featureMap);
  double maxOffset = 100.0;
  query->setMaxOffset(maxOffset);
  query->addExpression(*startTimeExpressionE1);
  query->addExpression(*targetE1Bait);
  query->addExpression(*startTimeExpressionE2_begin);
  query->addExpression(*startTimeExpressionE2_end);
  query->addExpression(*targetE2Controller);
  query->finalize();

  BOOST_CHECK_EQUAL(query->getMaxTimeExtent(), 110);
  BOOST_CHECK_EQUAL(query->getMaxOffset(), maxOffset);

  ResultType result(query, netflow1);

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

BOOST_FIXTURE_TEST_CASE( test_watering_hole, F )
{
  // Query
  // target e1 bait
  // endtime(e1) = 0;
  // target e2 controller;
  // starttime(e2) > 0;
  // starttime(e2) < 10;
  // bait in topk
  // controller not in topk

  std::string topkId = "topk";

  //bait in Topk
  VertexConstraintExpression baitTopK(bait, VertexOperator::In, topkId);

  //controller not in Topk
  VertexConstraintExpression controllerNotTopK(controller,
                                               VertexOperator::NotIn, topkId);


  auto query = std::make_shared<QueryType>(featureMap);
  double maxOffset = 100.0;
  query->setMaxOffset(maxOffset);
  query->addExpression(*endTimeExpressionE1);
  query->addExpression(*targetE1Bait);
  query->addExpression(*startTimeExpressionE2_begin);
  query->addExpression(*startTimeExpressionE2_end);
  query->addExpression(*targetE2Controller);
  query->addExpression(baitTopK);
  query->addExpression(controllerNotTopK);
  query->finalize();
  BOOST_CHECK_EQUAL(query->getMaxTimeExtent(), 110);
  BOOST_CHECK_EQUAL(query->getMaxOffset(), maxOffset);

  BOOST_CHECK_THROW(ResultType(query, netflow1), SubgraphQueryResultException);
   
  // TopKFeature 
  std::vector<std::string> keys;
  std::vector<double> frequencies;
  keys.push_back(bait);
  frequencies.push_back(0.8);
  TopKFeature feature(keys, frequencies); 

  featureMap->updateInsert("", topkId, feature);
  
  ResultType result(query, netflow1);

  double netflow1Time = std::get<TimeSeconds>(netflow1);
  double duration = std::get<DurationSeconds>(netflow1);
  double expireTime = netflow1Time + duration + maxOffset + 10;
  BOOST_CHECK_EQUAL(result.getExpireTime(), expireTime);

  BOOST_CHECK(!result.complete());

  auto pair = result.addEdge(netflow2);
  BOOST_CHECK(pair.first);
  BOOST_CHECK(pair.second.complete());

  double currentTime = std::get<TimeSeconds>(netflow3);

  BOOST_CHECK_EQUAL(pair.second.getExpireTime(), expireTime); 
  
}
