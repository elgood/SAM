#define BOOST_TEST_MAIN TestCollapsedFeatureMap
#include <boost/test/unit_test.hpp>
#include "CollapsedFeatureMap.hpp"
#include <thread>

using namespace sam;

BOOST_AUTO_TEST_CASE( single_feature )
{
  CollapsedFeatureMap collapsedFeatureMap;
  std::string key1 = "key1";
  std::string key2 = "key2";
  std::string featureName = "featureName";

  collapsedFeatureMap.updateInsert(key1, "key2", featureName, SingleFeature(5));
  collapsedFeatureMap.updateInsert(key1, "key3", featureName, SingleFeature(6));
  collapsedFeatureMap.updateInsert(key2, "key2", featureName, SingleFeature(1));

  auto sumFunction = [](std::list<std::shared_ptr<Feature>> mylist)->double {
    double sum = 0;
    for (auto feature : mylist) {

      double d = feature->evaluate(valueFunc);
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
   
  collapsedFeatureMap.updateInsert(key1, "key2", featureName, SingleFeature(1));
  b = collapsedFeatureMap.applyAggregate(key1, featureName, 
                                              sumFunction, result);
  BOOST_CHECK_EQUAL(b, true);
  BOOST_CHECK_EQUAL(result, 7);
 
  b = collapsedFeatureMap.applyAggregate("blah", featureName, 
                                              sumFunction, result);
  BOOST_CHECK_EQUAL(b, false);
  BOOST_CHECK_EQUAL(result, 0);
 
}

BOOST_AUTO_TEST_CASE( lots_of_threads )
{
  std::string destIp1 = "192.168.0.1";
  std::string destIp2 = "192.168.0.2";
  
  int numThreads = 10;
  std::vector<std::thread> threads; 

  for (int i = 0; i < numThreads; i++) 
  {
/*    threads.push_back(std::thread([]() {


    }));
*/
  }

}
