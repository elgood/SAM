#define BOOST_TEST_MAIN TestActiveWindow
#include <boost/test/unit_test.hpp>
#include "Features.hpp"

using namespace sam;


BOOST_AUTO_TEST_CASE( single_feature )
{
  SingleFeature feature(10.5);
  BOOST_CHECK_EQUAL(feature.evaluate(valueFunc), 10.5);
   
}

BOOST_AUTO_TEST_CASE( topk_feature )
{
  std::vector<std::string> keys;
  keys.push_back("1");
  keys.push_back("2");
  std::vector<double> frequencies;
  frequencies.push_back(0.4);
  frequencies.push_back(0.2);
  TopKFeature top2(keys, frequencies);
  
  int index = 0;

  auto topkValueFunction = [index](Feature const * feature)->double
  {
    auto topkFeature = static_cast<TopKFeature const *>(feature);
    return topkFeature->getFrequencies()[0];
  };
  
  BOOST_CHECK_EQUAL(top2.evaluate(topkValueFunction),0.4);
  
}

BOOST_AUTO_TEST_CASE( test_equality )
{
  BooleanFeature bf1(true);
  BooleanFeature bf2(true);
  BooleanFeature bf3(false);
  BOOST_CHECK(bf1 == bf2);
  BOOST_CHECK(bf1 != bf3);

  SingleFeature sf1(0.5);
  SingleFeature sf2(0.5);
  SingleFeature sf3(0.33);
  BOOST_CHECK(sf1 == sf2);
  BOOST_CHECK(sf1 != sf3);

  std::vector<std::string> keys1;
  keys1.push_back("1");
  keys1.push_back("2");
  std::vector<double> frequencies1;
  frequencies1.push_back(0.4);
  frequencies1.push_back(0.2);
  TopKFeature topk1(keys1, frequencies1);
 
  std::vector<std::string> keys2;
  keys2.push_back("1");
  keys2.push_back("2");
  std::vector<double> frequencies2;
  frequencies2.push_back(0.4);
  frequencies2.push_back(0.2);
  TopKFeature topk2(keys2, frequencies2);
 
  std::vector<std::string> keys3;
  keys3.push_back("1");
  keys3.push_back("2");
  std::vector<double> frequencies3;
  frequencies3.push_back(0.6);
  frequencies3.push_back(0.2);
  TopKFeature topk3(keys3, frequencies3);

  BOOST_CHECK(topk1 == topk2);
  BOOST_CHECK(topk1 != topk3);

  BOOST_CHECK(bf1 != sf1);
  BOOST_CHECK(bf1 != topk1);
  BOOST_CHECK(sf1 != topk1);
 
}
