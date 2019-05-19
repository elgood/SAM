#define BOOST_TEST_MAIN TestVertexConstraintChecker

#include <boost/test/unit_test.hpp>
#include <sam/VertexConstraintChecker.hpp>
#include <sam/SubgraphQueryResult.hpp>
#include <sam/SubgraphQuery.hpp>
#include <sam/FeatureMap.hpp>
#include <sam/EdgeDescription.hpp>

using namespace sam;
using namespace sam;

typedef SubgraphQuery<VastNetflow, SourceIp, DestIp, TimeSeconds, 
                      DurationSeconds> SubgraphQueryType;
typedef SubgraphQueryResult<VastNetflow, SourceIp, DestIp, TimeSeconds, 
                            DurationSeconds> SubgraphQueryResultType;
typedef VertexConstraintChecker<SubgraphQueryType> CheckerType;
        

struct F 
{
  std::shared_ptr<FeatureMap> featureMap;
  std::shared_ptr<SubgraphQueryType> query;
  std::string alice = "alice";
  std::string e0    = "e0";
  std::string bob   = "bob";
  std::string featureName = "topk";
  VertexConstraintExpression* inExpression;
  VertexConstraintExpression* notInExpression;

  F()
  {
    EdgeExpression edgeExpression(alice, e0, bob);
    TimeEdgeExpression timeExpression(EdgeFunction::StartTime, e0, 
                                      EdgeOperator::Assignment, 0);

    inExpression = new VertexConstraintExpression(alice, 
                                          VertexOperator::In,
                                          featureName);
    notInExpression = new VertexConstraintExpression(alice, 
                                          VertexOperator::NotIn,
                                          featureName);

    featureMap = std::make_shared<FeatureMap>();
    query = std::make_shared<SubgraphQueryType>(featureMap);
    query->addExpression(edgeExpression);
    query->addExpression(timeExpression);
  }

  ~F()
  {
    delete inExpression;
    delete notInExpression;
  }

};

BOOST_FIXTURE_TEST_CASE( test_check_vertex_nothing, F )
{
  query->addExpression(*inExpression);
  query->finalize();
  CheckerType checkVertex(featureMap, query.get());

  BOOST_CHECK_EQUAL(checkVertex(alice, "Alice"), false);

}

BOOST_FIXTURE_TEST_CASE( test_check_vertex_in, F )
{
  query->addExpression(*inExpression);
  query->finalize();
  std::string vertex = "Alice";
  CheckerType checkVertex(featureMap, query.get());
  
  std::vector<std::string> keys;
  keys.push_back(vertex);
  
  std::vector<double> frequencies;
  frequencies.push_back(0.5);
  TopKFeature feature(keys, frequencies);
  featureMap->updateInsert("", featureName, feature);

  BOOST_CHECK_EQUAL(checkVertex(alice, vertex), true);
}

BOOST_FIXTURE_TEST_CASE( test_check_vertex_notin, F )
{
  query->addExpression(*notInExpression);
  query->finalize();
  std::string vertex = "Alice";
  CheckerType checkVertex(featureMap, query.get());
  
  std::vector<std::string> keys;
  keys.push_back(vertex);
  
  std::vector<double> frequencies;
  frequencies.push_back(0.5);
  TopKFeature feature(keys, frequencies);
  featureMap->updateInsert("", featureName, feature);

  BOOST_CHECK_EQUAL(checkVertex(alice, vertex), false);
  BOOST_CHECK_EQUAL(checkVertex(alice, "Bob"), true);
}

