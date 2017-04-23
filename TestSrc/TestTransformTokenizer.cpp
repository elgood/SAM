#define BOOST_TEST_MAIN TestFilterTokenizer
#include <string>
#include <vector>
#include <boost/test/unit_test.hpp>
#include "ExpressionTokenizer.hpp"
#include "Grammars.hpp"

using std::string;
using std::vector;

using namespace sam;

BOOST_AUTO_TEST_CASE( function_test )
{
  string str = "prev(1).DestIp";
  ExpressionTokenizer<TransformGrammar<std::string::const_iterator>>
    tokenizer(str);
  //std::shared_ptr<FunctionToken> token = 
  //  std::static_pointer_cast<FunctionToken>(tokenizer.get(0));
  //BOOST_CHECK_EQUAL(token->getParameter(0), 0);
  //BOOST_CHECK_EQUAL(token->getParameter(1), 2);
}
