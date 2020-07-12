#define BOOST_TEST_MAIN TestLabel
#include <boost/test/unit_test.hpp>
#include <sam/Label.hpp>

using namespace sam;

BOOST_AUTO_TEST_CASE( test_extract_one_int_label )
{
  typedef std::tuple<int> LabelType;
  std::string s = "1,2,3,therest";
  LabelResult<LabelType> result = extractLabel<LabelType>(s);
  BOOST_CHECK_EQUAL(result.remainder, "2,3,therest");
  BOOST_CHECK_EQUAL(std::get<0>(result.label), 1);
}

BOOST_AUTO_TEST_CASE( test_extract_int_label )
{
  typedef std::tuple<int, int, int> LabelType;

  std::string s = "1,2,3,therest";

  LabelResult<LabelType> result = extractLabel<LabelType>(s);
  BOOST_CHECK_EQUAL(result.remainder, "therest");
  BOOST_CHECK_EQUAL(std::get<0>(result.label), 1);
  BOOST_CHECK_EQUAL(std::get<1>(result.label), 2);
  BOOST_CHECK_EQUAL(std::get<2>(result.label), 3);
}

BOOST_AUTO_TEST_CASE( test_mixed_label )
{
  typedef std::tuple<int, float, double> LabelType;

  std::string s = "1,2.5,3.6,therest";

  LabelResult<LabelType> result = extractLabel<LabelType>(s);
  BOOST_CHECK_EQUAL(result.remainder, "therest");
  BOOST_CHECK_EQUAL(std::get<0>(result.label), 1);
  BOOST_CHECK_EQUAL(std::get<1>(result.label), 2.5);
  BOOST_CHECK_EQUAL(std::get<2>(result.label), 3.6);


}
