#define BOOST_TEST_MAIN TestSubgraphQuery
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include <sam/SubgraphQuery.hpp>
#include <sam/VastNetflow.hpp>
#include <sam/FeatureMap.hpp>

using namespace sam;

typedef EdgeDescription<VastNetflow, TimeSeconds, DurationSeconds> 
  EdgeDescriptionType;
typedef SubgraphQuery<VastNetflow, SourceIp, DestIp, TimeSeconds, DurationSeconds>
  QueryType;

struct F {
  std::shared_ptr<FeatureMap> featureMap;

  F() {
    featureMap = std::make_shared<FeatureMap>();
  }
};

BOOST_FIXTURE_TEST_CASE( test_bad_finalize_no_source_target, F )
{
  // Prepares this subgraph query:
  // endtime(e1) = 0;
  // target1 e1 bait;
  // target1 e2 controller;
  // starttime(e2) > 10 
  // target1 e3 controller
  // starttime(e3) > 1 

  QueryType query(featureMap);

  // endtime(e1) = 0;
  std::string e1 = "e1";
  EdgeFunction endtime_e1 = EdgeFunction::EndTime;
  EdgeOperator equal_edge_operator = EdgeOperator::Assignment;
  double endtime_e1_value = 0;
  TimeEdgeExpression endTimeExpressionE1(endtime_e1, e1, equal_edge_operator, 
                                endtime_e1_value);
  
  query.addExpression(endTimeExpressionE1);

  // Adds a single TimeEdgeExpression to a SubgraphQuery and
  // then finalizes the query, which should result in an exception
  // because the source and target have not been specified.
  BOOST_CHECK_THROW(query.finalize(), SubgraphQueryException);

  // Adding an actual edge expression:
  // target1 e1 bait;
  std::string target1 = "target1";
  std::string bait = "bait";
  EdgeExpression target1E1Bait(target1, e1, bait);

  query.addExpression(target1E1Bait);

  // target1 e2 controller;
  std::string e2 = "e2";
  std::string controller = "controller";
  EdgeExpression targetE2Controller(target1, e2, controller);
  
  query.addExpression(targetE2Controller);
 
  // starttime(e2) > 10 
  EdgeFunction starttime_e2 = EdgeFunction::StartTime;
  EdgeOperator greater_edge_operator = EdgeOperator::GreaterThan;
  double starttime_e2_value = 10;
  TimeEdgeExpression starttimeExpressionE2(starttime_e2, e2, 
                                  greater_edge_operator, 
                                  starttime_e2_value);  
  query.addExpression(starttimeExpressionE2);

  // target1 e3 controller
  std::string e3 = "e3";
  EdgeExpression targetE3Controller(target1, e3, controller);
  query.addExpression(targetE3Controller);
 
  // starttime(e3) > 1 
  EdgeFunction starttime_e3 = EdgeFunction::StartTime;
  double starttime_e3_value = 1;
  TimeEdgeExpression starttimeExpressionE3(starttime_e3, e3, 
                                  greater_edge_operator, 
                                  starttime_e3_value);  
  query.addExpression(starttimeExpressionE3);

  query.finalize(); 

  // Checking that the edge are in temporal order
  double prev = std::numeric_limits<double>::lowest();
  int i = 0;
  for (EdgeDescription<VastNetflow, TimeSeconds, DurationSeconds> edge : query) {
    switch (i)
    {
      case(0): BOOST_CHECK_EQUAL(e1, edge.edgeId); break;
      case(1): BOOST_CHECK_EQUAL(e3, edge.edgeId); break;
      case(2): BOOST_CHECK_EQUAL(e2, edge.edgeId); break;
      default: BOOST_CHECK(false);
    }
    if (prev > edge.startTimeRange.first) {
      BOOST_CHECK(false);  
    }
    prev = edge.startTimeRange.first;
    i++;
  }
}

BOOST_FIXTURE_TEST_CASE( test_negative_offset, F )
{
  QueryType query(featureMap);
  BOOST_CHECK_THROW(query.setMaxOffset(-1), SubgraphQueryException);
}

BOOST_FIXTURE_TEST_CASE( test_unspecified_startendtime, F )
{
  QueryType query(featureMap);

  std::string target1 = "target1";
  std::string e1 = "e1";
  std::string bait = "bait";
  EdgeExpression target1E1Bait(target1, e1, bait);

  query.addExpression(target1E1Bait);
 
  // All edges need at least an end time or a start time 
  BOOST_CHECK_THROW(query.finalize(), EdgeDescriptionException); 
}

BOOST_FIXTURE_TEST_CASE( test_conflicting_sources, F )
{
  QueryType query(featureMap);
  
  std::string target1 = "target1";
  std::string e1 = "e1";
  std::string bait = "bait";
  EdgeExpression target1E1Bait(target1, e1, bait);
  
  std::string target2 = "target2";
  EdgeExpression target2E1Bait(target2, e1, bait);

  query.addExpression(target1E1Bait);
  BOOST_CHECK_THROW(query.addExpression(target2E1Bait), SubgraphQueryException);
}

// "target e1 bait;
// endtime(e1) = 0;
// target e2 controller;
// starttime(e2) > 0;
// starttime(e2) < 10;
// bait in Top1000;
// controller not in Top1000;"
BOOST_FIXTURE_TEST_CASE( test_watering_hole, F )
{
  std::string target = "target";
  std::string e1 = "e1";
  std::string bait = "bait";
  EdgeExpression targetE1Bait(target, e1, bait);

  EdgeFunction endtime_e1 = EdgeFunction::EndTime;
  EdgeOperator equal_edge_operator = EdgeOperator::Assignment;
  double endtime_e1_value = 0;
  TimeEdgeExpression endtimeExpressionE1(endtime_e1, e1, equal_edge_operator, 
                                endtime_e1_value); 
  
  std::string e2 = "e2";
  std::string controller = "controller";
  EdgeExpression targetE2Controller(target, e2, controller);
  
  EdgeFunction starttime_e2_begin = EdgeFunction::StartTime;
  EdgeOperator greater_edge_operator = EdgeOperator::GreaterThan;
  double starttime_e2_value_begin = 0;
  TimeEdgeExpression starttimeExpressionE2(starttime_e2_begin, e2, 
                                  greater_edge_operator, 
                                  starttime_e2_value_begin);  


  EdgeFunction starttime_e2_end = EdgeFunction::StartTime;
  EdgeOperator less_edge_operator = EdgeOperator::LessThan;
  double starttime_e2_value_end = 10;
  TimeEdgeExpression endtimeExpressionE2(starttime_e2_end, e2, 
                     less_edge_operator, 
                     starttime_e2_value_end);  

  QueryType query(featureMap);
  double maxOffset = 15.0;
  query.setMaxOffset(maxOffset);
  query.addExpression(targetE1Bait);
  query.addExpression(endtimeExpressionE1);
  query.addExpression(targetE2Controller);
  query.addExpression(starttimeExpressionE2);
  query.addExpression(endtimeExpressionE2);
  query.finalize();

  EdgeDescription<VastNetflow, TimeSeconds, DurationSeconds> const& edge0 = 
    query.getEdgeDescription(0);
  EdgeDescription<VastNetflow, TimeSeconds, DurationSeconds> const& edge1 = 
    query.getEdgeDescription(1);

  BOOST_CHECK_EQUAL(edge0.startTimeRange.first, endtime_e1_value-maxOffset);
  BOOST_CHECK_EQUAL(edge0.startTimeRange.second, endtime_e1_value);
  BOOST_CHECK_EQUAL(edge0.endTimeRange.first, endtime_e1_value);
  BOOST_CHECK_EQUAL(edge0.endTimeRange.second, endtime_e1_value); 
  BOOST_CHECK_EQUAL(edge1.startTimeRange.first, starttime_e2_value_begin);
  BOOST_CHECK_EQUAL(edge1.startTimeRange.second, starttime_e2_value_end);
  BOOST_CHECK_EQUAL(edge1.endTimeRange.first, starttime_e2_value_begin);
  BOOST_CHECK_EQUAL(edge1.endTimeRange.second, starttime_e2_value_end
    + maxOffset);

  // Checking the maxTimeExtent, which should be final edge endtime - 
  // first edge endtime
  BOOST_CHECK_EQUAL(query.getMaxTimeExtent(), 
    starttime_e2_value_end + maxOffset -
    (endtime_e1_value));
}

BOOST_FIXTURE_TEST_CASE( test_defined_start_undefined_end, F )
{
  // Here we test setting the start time but not the end time.  The 
  
  std::string x = "x";
  std::string e = "e";
  std::string y = "y";
  EdgeExpression edge(x, e, y);
  EdgeFunction starttime = EdgeFunction::StartTime;
  EdgeOperator equal_edge_operator = EdgeOperator::Assignment;
  double starttime_e1_value = 0;
  TimeEdgeExpression starttimeExpression(starttime, e, equal_edge_operator, 
                                starttime_e1_value);
  
   
  QueryType query(featureMap);
  query.addExpression(edge);
  query.addExpression(starttimeExpression);
 
  double maxOffset = 50;
  query.setMaxOffset(50);
  query.finalize();

  EdgeDescriptionType const& edgeDesc = query.getEdgeDescription(0);

  BOOST_CHECK_EQUAL(edgeDesc.getSource(), x);
  BOOST_CHECK_EQUAL(edgeDesc.getEdgeId(), e);
  BOOST_CHECK_EQUAL(edgeDesc.getTarget(), y);
  BOOST_CHECK_EQUAL(edgeDesc.startTimeRange.first, 0);
  BOOST_CHECK_EQUAL(edgeDesc.startTimeRange.second, 0);
  BOOST_CHECK_EQUAL(edgeDesc.endTimeRange.first, 0);
  BOOST_CHECK_EQUAL(edgeDesc.endTimeRange.second, maxOffset);

}
