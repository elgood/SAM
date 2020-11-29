#define BOOST_TEST_MAIN TestTokens
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <stack>
#include <tuple>
#include <sam/Tokens.hpp>
#include <sam/tuples/VastNetflow.hpp>
#include <sam/FeatureMap.hpp>

using namespace sam;
using namespace sam::vast_netflow;

struct F {
  VastNetflow netflow;
  std::shared_ptr<FeatureMap> featureMap = std::make_shared<FeatureMap>();
  std::stack<double> mystack;
  std::string key;

  F() {
    std::string s = "1365582756.384094,2013-04-10 08:32:36,"
                    "20130410083236.384094,17,UDP,172.20.2.18,"
                    "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";
    netflow = makeVastNetflow(s);
    key = "key";
  }

  ~F() {
  }

};

BOOST_FIXTURE_TEST_CASE( test_number_token, F )
{
  NumberToken<VastNetflow> number(featureMap, DestIp);

  bool b = number.evaluate(mystack, key, netflow);
  BOOST_CHECK_EQUAL(b, true);
  BOOST_CHECK_EQUAL(mystack.top(), DestIp);

}
 
BOOST_FIXTURE_TEST_CASE( test_add_token, F )
{
  mystack.push(1.6);
  mystack.push(3.5);

  AddOperator<VastNetflow> addOper(featureMap);

  bool b = addOper.evaluate(mystack, key, netflow);
  BOOST_CHECK_EQUAL(b, true);
  BOOST_CHECK_EQUAL(mystack.top(), 5.1);

  b = addOper.evaluate(mystack, key, netflow);
  BOOST_CHECK_EQUAL(b, false);

}
 
BOOST_FIXTURE_TEST_CASE( test_sub_token, F )
{
  mystack.push(1.6);
  mystack.push(3.5);

  SubOperator<VastNetflow> subOper(featureMap);

  bool b = subOper.evaluate(mystack, key, netflow);
  BOOST_CHECK_EQUAL(b, true);
  BOOST_CHECK_EQUAL(mystack.top(), -1.9);

  b = subOper.evaluate(mystack, key, netflow);
  BOOST_CHECK_EQUAL(b, false);

}
 
BOOST_FIXTURE_TEST_CASE( test_mult_token, F )
{
  mystack.push(3);
  mystack.push(2);

  MultOperator<VastNetflow> multOper(featureMap);

  bool b = multOper.evaluate(mystack, key, netflow);
  BOOST_CHECK_EQUAL(b, true);
  BOOST_CHECK_EQUAL(mystack.top(), 6);

  b = multOper.evaluate(mystack, key, netflow);
  BOOST_CHECK_EQUAL(b, false);

}

BOOST_FIXTURE_TEST_CASE( test_field_token, F )
{

  FieldToken<TimeSeconds, VastNetflow> fieldToken(featureMap);

  bool b = fieldToken.evaluate(mystack, key, netflow);
  BOOST_CHECK_EQUAL(b, true);
  BOOST_CHECK_EQUAL(mystack.top(), 1365582756.384094);
}

BOOST_FIXTURE_TEST_CASE( test_func_token, F )
{
  std::string identifier = "top2";
  std::string function   = "value";
  std::vector<double> parameters;
  parameters.push_back(1);

  auto func = [&function, &parameters](Feature const * feature)->double {
    auto topkFeature = static_cast<TopKFeature const *>(feature);
    if (function.compare(VALUE_FUNCTION) == 0) {
      if (parameters.size() != 1) {
        throw std::runtime_error("Expected there to be one parameter," 
                    " found " +
                    boost::lexical_cast<std::string>(parameters.size())); 
      }
      int index = boost::lexical_cast<int>(parameters[0]);
      return topkFeature->getFrequencies()[index];
    }
    throw std::runtime_error("Evaluate with function " + function + 
      " is not defined for class TopKFeature");

  };



  FuncToken<VastNetflow> funcToken(featureMap,
                               func,
                               identifier);

  // Nothing in featureMap, so should return false.
  bool b = funcToken.evaluate(mystack, key, netflow);
  BOOST_CHECK_EQUAL(b, false);

  std::vector<std::string> keys;
  std::vector<double> frequencies;
  keys.push_back("key1");
  keys.push_back("key2");
  frequencies.push_back(.4);
  frequencies.push_back(.3);

  TopKFeature feature(keys, frequencies);

  featureMap->updateInsert(key, identifier, feature);
  
  b = funcToken.evaluate(mystack, key, netflow);
  BOOST_CHECK_EQUAL(b, true);
  BOOST_CHECK_EQUAL(mystack.top(), .3);

}

BOOST_FIXTURE_TEST_CASE( test_prev_token, F )
{
  PrevToken<TimeSeconds, VastNetflow> prevToken1(featureMap);
  PrevToken<TimeSeconds, VastNetflow> prevToken2(featureMap);

  // We don't want the identifiers to be the same or that could cause problems.
  BOOST_CHECK(prevToken1.getIdentifier() != prevToken2.getIdentifier());

  // First pass should have no previous value, so fails.
  bool b = prevToken1.evaluate(mystack, key, netflow); 
  BOOST_CHECK_EQUAL(b, false);

  // Second pass should have previous value.
  b = prevToken1.evaluate(mystack, key, netflow); 
  BOOST_CHECK_EQUAL(b, true);
  BOOST_CHECK_EQUAL(mystack.top(), 1365582756.384094);


}

 
