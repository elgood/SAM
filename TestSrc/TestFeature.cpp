#define BOOST_TEST_MAIN TestActiveWindow
#include <boost/test/unit_test.hpp>
#include "Features.hpp"

using namespace sam;


BOOST_AUTO_TEST_CASE( single_feature )
{
  SingleFeature feature(10.5);
  BOOST_CHECK_EQUAL(feature.evaluate(), 10.5);
  BOOST_CHECK_EQUAL(feature.evaluate(VALUE_FUNCTION, 
                    std::vector<double>()), 10.5);
  BOOST_CHECK_THROW(feature.evaluate("blah", std::vector<double>()),
                    std::runtime_error);
   
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
  
  std::vector<double> parameters;
  parameters.push_back(0);
  BOOST_CHECK_EQUAL(top2.evaluate(VALUE_FUNCTION,
                    parameters), 0.4);
  
  BOOST_CHECK_THROW(top2.evaluate(), std::runtime_error); 
}
