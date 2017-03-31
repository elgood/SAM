#define BOOST_TEST_MAIN TestFilterExpression
#include <string>
#include <vector>
#include <boost/test/unit_test.hpp>
#include "FilterExpression.hpp"

using std::string;
using std::vector;

using namespace sam;

BOOST_AUTO_TEST_CASE( number_test )
{
  string str = "1.0 + 2.5";
  FilterExpression expression(str);
  ImuxDataItem item;
  BOOST_CHECK_EQUAL(expression.evaluate(item), 3.5);
}

BOOST_AUTO_TEST_CASE( compariton_test )
{
  string str = "top2.value(0) + top2.value(1) > 0.9";
  FilterExpression expression(str);
  ImuxDataItem item;
  std::vector<std::string> keys;
  keys.push_back("1");
  keys.push_back("2");
  std::vector<double> frequencies;
  frequencies.push_back(0.85);
  frequencies.push_back(0.1);
  std::shared_ptr<TopKFeature> feature(new TopKFeature(keys, frequencies));
  item.addFeature("top2", feature); 
  BOOST_CHECK_EQUAL(expression.evaluate(item), 1);
}


/*BOOST_AUTO_TEST_CASE( operator_associativity )
{
  // Should throw an exception because the associativity is not valid.
  BOOST_CHECK_THROW(Operator(3, 3), std::invalid_argument);

  int prec1 = 0;
  Operator op1(Operator::RIGHT_ASSOCIATIVE, prec1);
  BOOST_CHECK_EQUAL(op1.getAssociativity(), Operator::RIGHT_ASSOCIATIVE);
  BOOST_CHECK_EQUAL(op1.getPrecedence(), prec1);

  int prec2 = 4;
  Operator op2(Operator::LEFT_ASSOCIATIVE, prec2);
  BOOST_CHECK_EQUAL(op2.getAssociativity(), Operator::LEFT_ASSOCIATIVE);
  BOOST_CHECK_EQUAL(op2.getPrecedence(), prec2);
}*/


/*BOOST_AUTO_TEST_CASE( operator_precedence )
{
  BOOST_CHECK_EQUAL(Minus().getPrecedence(), 2);
  BOOST_CHECK_EQUAL(Plus().getPrecedence(), 2);
  BOOST_CHECK_EQUAL(Divide().getPrecedence(), 3);
  BOOST_CHECK_EQUAL(Multiply().getPrecedence(), 3);
  BOOST_CHECK_EQUAL(Power().getPrecedence(), 4);
}*/

