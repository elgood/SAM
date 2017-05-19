#define BOOST_TEST_MAIN TestCollapsedFeatureMap
#include <boost/test/unit_test.hpp>
#include "CollapsedFeatureMap.hpp"

using namespace sam;


BOOST_AUTO_TEST_CASE( single_feature )
{
  CollapsedFeatureMap collapsedFeatureMap;
  std::string key1 = "key1";
  std::string key2 = "key2";
  std::string featureName = "featureName";

  collapsedFeatureMap.updateInsert(key1, "key2", featureName, 5);
  collapsedFeatureMap.updateInsert(key1, "key3", featureName, 6);
  collapsedFeatureMap.updateInsert(key2, "key2", featureName, 1);

  auto sumFunction = [](std::list<double> mylist)->double {
    double sum = 0;
    for (double d : mylist) {
      sum = sum + d;
    }
    return sum;
  };

  double result;
  bool b = collapsedFeatureMap.applyAggregate("key1", featureName, 
                                              sumFunction, result);

  BOOST_CHECK_EQUAL(b, true);
  BOOST_CHECK_EQUAL(result, 11);
   
  b = collapsedFeatureMap.applyAggregate("key2", featureName, 
                                              sumFunction, result);
  BOOST_CHECK_EQUAL(b, true);
  BOOST_CHECK_EQUAL(result, 1);
   
  collapsedFeatureMap.updateInsert(key1, "key2", featureName, 1);
  b = collapsedFeatureMap.applyAggregate(key1, featureName, 
                                              sumFunction, result);
  BOOST_CHECK_EQUAL(b, true);
  BOOST_CHECK_EQUAL(result, 7);
 
  b = collapsedFeatureMap.applyAggregate("blah", featureName, 
                                              sumFunction, result);
  BOOST_CHECK_EQUAL(b, false);
  BOOST_CHECK_EQUAL(result, 0);
 
}
