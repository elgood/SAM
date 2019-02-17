#define BOOST_TEST_MAIN TestNull

#include <sam/Null.hpp>
#include <string>
#include <boost/test/unit_test.hpp>


using namespace sam;

BOOST_AUTO_TEST_CASE( test_string_null )
{
  std::string nullString = nullValue<std::string>();
  BOOST_CHECK_EQUAL(nullString, ""); 

  BOOST_CHECK(isNull(nullString));

  std::string blah = "blah";
  BOOST_CHECK(!isNull(blah));
}

