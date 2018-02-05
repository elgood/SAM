#define BOOST_TEST_MAIN TestSubgraphQuery
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include "SubgraphQuery.hpp"

using namespace sam;

BOOST_AUTO_TEST_CASE( test_edge_description )
{
  /// Tests unspecifiedSource/Target
  EdgeDescription e;
  BOOST_CHECK_EQUAL(e.unspecifiedSource(), true);
  BOOST_CHECK_EQUAL(e.unspecifiedTarget(), true);
  e.source = "192.168.0.1";
  BOOST_CHECK_EQUAL(e.unspecifiedSource(), false);
  BOOST_CHECK_EQUAL(e.unspecifiedTarget(), true);
  e.target = "192.168.0.1";
  BOOST_CHECK_EQUAL(e.unspecifiedSource(), false);
  BOOST_CHECK_EQUAL(e.unspecifiedTarget(), false);
}

BOOST_AUTO_TEST_CASE( test_bad_finalize_no_source_target )
{
  /// Adds a single TimeEdgeExpression to a SubgraphQuery and
  /// then finalizes the query, which should result in an exception
  /// because the source and target have not been specified.
  SubgraphQuery query;

  std::string e1 = "e1";
  EdgeFunction endtime_e1 = EdgeFunction::EndTime;
  EdgeOperator equal_edge_operator = EdgeOperator::Assignment;
  double endtime_e1_value = 0;
  TimeEdgeExpression endTimeExpressionE1(endtime_e1, e1, equal_edge_operator, 
                                endtime_e1_value);
  
  query.addExpression(endTimeExpressionE1);

  BOOST_CHECK_THROW(query.finalize(), SubgraphQueryException);

  std::string target1 = "target1";
  std::string bait = "bait";
  EdgeExpression target1E1Bait(target1, e1, bait);

  query.addExpression(target1E1Bait);
 
  std::string e2 = "e2";
  std::string controller = "controller";
  EdgeExpression targetE2Controller(target1, e2, controller);
  
  query.addExpression(targetE2Controller);
  
  EdgeFunction starttime_e2 = EdgeFunction::StartTime;
  EdgeOperator greater_edge_operator = EdgeOperator::GreaterThan;
  double starttime_e2_value = 10;
  TimeEdgeExpression starttimeExpressionE2(starttime_e2, e2, 
                                  greater_edge_operator, 
                                  starttime_e2_value);  
  query.addExpression(starttimeExpressionE2);

  std::string e3 = "e3";
  EdgeExpression targetE3Controller(target1, e3, controller);
  query.addExpression(targetE3Controller);
  
  EdgeFunction starttime_e3 = EdgeFunction::StartTime;
  double starttime_e3_value = 1;
  TimeEdgeExpression starttimeExpressionE3(starttime_e3, e3, 
                                  greater_edge_operator, 
                                  starttime_e3_value);  
  query.addExpression(starttimeExpressionE3);

  query.finalize(); 

  // Checking that the edge are in temporal order
  double prev = 0;
  int i = 0;
  for (EdgeDescription edge : query) {
    switch (i)
    {
      case(0): BOOST_CHECK_EQUAL(e1, edge.edgeId); break;
      case(1): BOOST_CHECK_EQUAL(e3, edge.edgeId); break;
      case(2): BOOST_CHECK_EQUAL(e2, edge.edgeId); break;
      default: BOOST_CHECK(false);
    }
    if (prev > edge.startTime) {
      BOOST_CHECK(false);  
    }
    prev = edge.startTime;
    i++;
  }
}

BOOST_AUTO_TEST_CASE( test_negative_offset )
{
  SubgraphQuery query;
  BOOST_CHECK_THROW(query.setMaxOffset(-1), SubgraphQueryException);
}

BOOST_AUTO_TEST_CASE( test_unspecified_startendtime )
{
  SubgraphQuery query;

  std::string target1 = "target1";
  std::string e1 = "e1";
  std::string bait = "bait";
  EdgeExpression target1E1Bait(target1, e1, bait);

  query.addExpression(target1E1Bait);
 
  // All edges need at least an end time or a start time 
  BOOST_CHECK_THROW(query.finalize(), SubgraphQueryException); 
}

BOOST_AUTO_TEST_CASE( test_conflicting_sources )
{
  SubgraphQuery query;
  
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
// endtime(e2) < 10;
// bait in Top1000;
// controller not in Top1000;"
BOOST_AUTO_TEST_CASE( test_watering_hole )
{
  std::string target = "target";
  std::string e1 = "e1";
  std::string bait = "bait";
  EdgeExpression targetE1Bait(target, e1, bait);

  EdgeFunction endtime_e1 = EdgeFunction::EndTime;
  EdgeOperator equal_edge_operator = EdgeOperator::Equal;
  double endtime_e1_value = 0;
  TimeEdgeExpression endtimeExpressionE1(endtime_e1, e1, equal_edge_operator, 
                                endtime_e1_value); 
  
  std::string e2 = "e2";
  std::string controller = "controller";
  EdgeExpression targetE2Controller(target, e2, controller);
  
  EdgeFunction starttime_e2 = EdgeFunction::StartTime;
  EdgeOperator greater_edge_operator = EdgeOperator::GreaterThan;
  double starttime_e2_value = 0;
  TimeEdgeExpression starttimeExpressionE2(starttime_e2, e2, 
                                  greater_edge_operator, 
                                  starttime_e2_value);  


  EdgeFunction endtime_e2 = EdgeFunction::StartTime;
  EdgeOperator less_edge_operator = EdgeOperator::Equal;
  double endtime_e2_value = 10;
  TimeEdgeExpression endtimeExpressionE2(starttime_e2, e2, 
                     greater_edge_operator, 
                     starttime_e2_value);  


  
}
