#define BOOST_TEST_MAIN TestTokens
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <stack>
#include <tuple>
#include "Tokens.hpp"
#include "Netflow.hpp"
#include "FeatureMap.hpp"

using namespace sam;

struct F {
  Netflow netflow;
  FeatureMap featureMap;
  std::stack<double> mystack;
  std::string key;

  F() {
    std::string s = "1365582756.384094,2013-04-10 08:32:36,"
                    "20130410083236.384094,17,UDP,172.20.2.18,"
                    "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";
    netflow = makeNetflow(s);
    key = "key";
  }

  ~F() {
  }

};

BOOST_FIXTURE_TEST_CASE( test_number_token, F )
{
  NumberToken<Netflow> number(featureMap, 6);

  bool b = number.evaluate(mystack, key, netflow);
  BOOST_CHECK_EQUAL(b, true);
  BOOST_CHECK_EQUAL(mystack.top(), 6);

}
 
BOOST_FIXTURE_TEST_CASE( test_add_token, F )
{
  mystack.push(1.6);
  mystack.push(3.5);

  AddOperator<Netflow> addOper(featureMap);

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

  SubOperator<Netflow> subOper(featureMap);

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

  MultOperator<Netflow> multOper(featureMap);

  bool b = multOper.evaluate(mystack, key, netflow);
  BOOST_CHECK_EQUAL(b, true);
  BOOST_CHECK_EQUAL(mystack.top(), 6);

  b = multOper.evaluate(mystack, key, netflow);
  BOOST_CHECK_EQUAL(b, false);

}

BOOST_FIXTURE_TEST_CASE( test_field_token, F )
{

  FieldToken<0, Netflow> fieldToken(featureMap);

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

  FuncToken<Netflow> funcToken(featureMap,
                               identifier,
                               function,
                               parameters );

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

  featureMap.updateInsert(key, identifier, feature);
  
  b = funcToken.evaluate(mystack, key, netflow);
  BOOST_CHECK_EQUAL(b, true);
  BOOST_CHECK_EQUAL(mystack.top(), .3);

}

BOOST_FIXTURE_TEST_CASE( test_prev_token, F )
{
  PrevToken<TIME_SECONDS_FIELD, Netflow> prevToken(featureMap);

  // First pass should have no previous value, so fails.
  bool b = prevToken.evaluate(mystack, key, netflow); 
  BOOST_CHECK_EQUAL(b, false);

  // Second pass should have previous value.
  b = prevToken.evaluate(mystack, key, netflow); 
  BOOST_CHECK_EQUAL(b, true);
  BOOST_CHECK_EQUAL(mystack.top(), 1365582756.384094);
}

 
