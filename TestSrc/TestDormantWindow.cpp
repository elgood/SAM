#define BOOST_TEST_MAIN TestDormantWindow
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include "DormantWindow.hpp"
#include "ActiveWindow.hpp"

// This is the fixture that is setup before each test case.
struct SetUp {
  ActiveWindow<size_t> aw;

  // Creating an active window with a limit of 6
  SetUp() : aw(6)
  {
    // Adding test data to the active window 
    aw.update(1);
    aw.update(1);
    aw.update(1);
    aw.update(3);
    aw.update(3);
    aw.update(2);
  }

  ~SetUp() {}

};

BOOST_FIXTURE_TEST_CASE( dormant_window_test_constructor, SetUp )
{
  size_t k = 2;
  DormantWindow<size_t> dw(k, aw);

  BOOST_CHECK_EQUAL( k, dw.getLimit());

  std::pair<size_t, size_t> item = dw.getIthMostFrequent(0);
  BOOST_CHECK_EQUAL(item.first, 1);
  BOOST_CHECK_EQUAL(item.second, 3);

  item = dw.getIthMostFrequent(1);
  BOOST_CHECK_EQUAL(item.first, 3);
  BOOST_CHECK_EQUAL(item.second, 2);

  BOOST_CHECK_THROW(dw.getIthMostFrequent(2), std::out_of_range );
}
