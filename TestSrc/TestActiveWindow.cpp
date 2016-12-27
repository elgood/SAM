#define BOOST_TEST_MAIN TestActiveWindow
#include <boost/test/unit_test.hpp>
#include "ActiveWindow.hpp"

using namespace sam;

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

// This tests adding an element to a full active window.
// It should return false;
BOOST_FIXTURE_TEST_CASE( active_window_test_limit, SetUp )
{
  BOOST_CHECK_EQUAL(false, aw.update(1));
}

BOOST_FIXTURE_TEST_CASE( active_window_test_get_num_elements, SetUp )
{
  BOOST_CHECK_EQUAL(6, aw.getNumElements());
}

BOOST_FIXTURE_TEST_CASE( active_window_test_topk, SetUp )
{
  std::vector<std::pair<size_t, size_t>> list1 = aw.topk(1);
  std::vector<std::pair<size_t, size_t>> list2 = aw.topk(2);
  std::vector<std::pair<size_t, size_t>> list3 = aw.topk(3);
  std::vector<std::pair<size_t, size_t>> list4 = aw.topk(4);

  BOOST_CHECK_EQUAL(1, list1.size());
  BOOST_CHECK_EQUAL(2, list2.size());
  BOOST_CHECK_EQUAL(3, list3.size());
  BOOST_CHECK_EQUAL(3, list4.size());

  BOOST_CHECK_EQUAL(1, list1[0].first);
  BOOST_CHECK_EQUAL(3, list1[0].second);
  BOOST_CHECK_EQUAL(1, list2[0].first);
  BOOST_CHECK_EQUAL(3, list2[0].second);
  BOOST_CHECK_EQUAL(3, list2[1].first);
  BOOST_CHECK_EQUAL(2, list2[1].second);
  BOOST_CHECK_EQUAL(1, list3[0].first);
  BOOST_CHECK_EQUAL(3, list3[0].second);
  BOOST_CHECK_EQUAL(3, list3[1].first);
  BOOST_CHECK_EQUAL(2, list3[1].second);
  BOOST_CHECK_EQUAL(2, list3[2].first);
  BOOST_CHECK_EQUAL(1, list3[2].second);
  BOOST_CHECK_EQUAL(1, list4[0].first);
  BOOST_CHECK_EQUAL(3, list4[0].second);
  BOOST_CHECK_EQUAL(3, list4[1].first);
  BOOST_CHECK_EQUAL(2, list4[1].second);
  BOOST_CHECK_EQUAL(2, list4[2].first);
  BOOST_CHECK_EQUAL(1, list4[2].second);
}
