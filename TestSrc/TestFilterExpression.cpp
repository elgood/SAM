#define BOOST_TEST_MAIN TestExpression
#include <string>
#include <vector>
#include <boost/test/unit_test.hpp>
#include "Expression.hpp"

using std::string;
using std::vector;

using namespace sam;

BOOST_AUTO_TEST_CASE( number_test )
{
  string str = "1.0 + 2.5";
  Expression<FilterGrammar<std::string::const_iterator>> expression(str);
  FeatureMap featureMap;
  std::string key = "blah";
  BOOST_CHECK_EQUAL(expression.evaluate(key, featureMap), 3.5);
}

BOOST_AUTO_TEST_CASE( comparison_test )
{
  FeatureMap featureMap;
  std::vector<std::string> keys;
  keys.push_back("1");
  keys.push_back("2");
  std::vector<double> frequencies;
  frequencies.push_back(0.85);
  frequencies.push_back(0.1);
  TopKFeature feature(keys, frequencies);
  std::string key = "blah";
  std::string id  = "top2";
  featureMap.updateInsert(key, id, feature);
  string str = "top2.value(0) + top2.value(1) > 0.9";
  Expression<FilterGrammar<std::string::const_iterator>> expression(str);
  BOOST_CHECK_EQUAL(expression.evaluate(key, featureMap), 1);
}

/**
 * This checks that expression.evaluate throws an exception when there
 * is not the necessary data in the feature map.
 */
BOOST_AUTO_TEST_CASE ( empty_item )
{
  FeatureMap featureMap;
  std::string key = "blah";
  string str = "top2.value(0) + top2.value(1) > 0.9";
  Expression<FilterGrammar<std::string::const_iterator>> expression(str);
  BOOST_CHECK_THROW(expression.evaluate(key, featureMap), std::logic_error);
}


