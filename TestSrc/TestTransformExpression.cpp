#define BOOST_TEST_MAIN TestTransformExpression
#include <string>
#include <vector>
#include <boost/test/unit_test.hpp>
#include "FeatureMap.hpp"

using namespace sam;

BOOST_AUTO_TEST_CASE( prev_test )
{
  FeatureMap featureMap;
/*  std::vector<std::string> keys;
  keys.push_back("1");
  keys.push_back("2");
  std::vector<double> frequencies;
  frequencies.push_back(0.85);
  frequencies.push_back(0.1);
  TopKFeature feature(keys, frequencies);
  std::string key = "blah";
  std::string id  = "top2";
  featureMap.updateInsert(key, id, feature);
  string str = "TimeSeconds - Prev(1).TimeSeconds";
  Expression<TransformGrammar<std::string::const_iterator>> expression(str);
  BOOST_CHECK_EQUAL(expression.evaluate(key, featureMap), 1);
*/
}
