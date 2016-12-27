#define BOOST_TEST_MAIN TestSlidingWindow
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include "DormantWindow.hpp"
#include "ActiveWindow.hpp"
#include "SlidingWindow.hpp"

using namespace sam;

BOOST_AUTO_TEST_CASE( test_zero_dormant ) 
{
  BOOST_CHECK_THROW( SlidingWindow<size_t>(1, 2, 2), std::invalid_argument);
}

BOOST_AUTO_TEST_CASE( test_neg1_dormant )
{
  BOOST_CHECK_THROW( SlidingWindow<size_t>(-10, 2, 2), std::invalid_argument);
}

BOOST_AUTO_TEST_CASE( test_neg2_dormant )
{
  BOOST_CHECK_THROW( SlidingWindow<size_t>(10, -2, 2), std::invalid_argument);
}

BOOST_AUTO_TEST_CASE( test_evenly_divides_dormant )
{
  auto sw = SlidingWindow<size_t>(10, 2, 2);
  BOOST_CHECK_EQUAL(4, sw.getNumDormant());
}


BOOST_AUTO_TEST_CASE( test_unevenly_divides_dormant )
{
  auto sw = SlidingWindow<size_t>(10, 3, 2);
  BOOST_CHECK_EQUAL(2, sw.getNumDormant());
}

BOOST_AUTO_TEST_CASE( test_add_dormant )
{
  int N = 50;
  int b = 10;
  int k = 2;
  SlidingWindow<size_t> sw(N, b, k);
  sw.add(1);
  sw.add(1);
  sw.add(1);
  BOOST_CHECK_EQUAL(3, sw.getNumActiveElements());
  BOOST_CHECK_EQUAL(0, sw.getNumDormantElements());
  BOOST_CHECK_THROW( sw.getIthElement(0), std::out_of_range);
  for (int i = 0; i < 7; i++) {
    sw.add(1);
  }
  BOOST_CHECK_EQUAL(10, sw.getNumActiveElements());
  BOOST_CHECK_THROW(sw.getIthElement(0), std::out_of_range);
  
  // Should create dormant window
  sw.add(1);
  BOOST_CHECK_EQUAL(1, sw.getNumActiveElements());
  BOOST_CHECK_EQUAL(10, sw.getNumDormantElements());
  BOOST_CHECK_EQUAL(1, sw.getIthElement(0).first);
  BOOST_CHECK_EQUAL(10, sw.getIthElement(0).second);
  BOOST_CHECK_THROW(sw.getIthElement(1), std::out_of_range);
  
  // Filling the rest of the current active window
  // One dormant and one full active
  for (int i = 0; i < 4; i++) sw.add(1);
  for (int i = 0; i < 5; i++) sw.add(2);
  BOOST_CHECK_EQUAL(10, sw.getNumActiveElements());
  
  // Should create dormant window number 2
  sw.add(3);
  sw.add(4);
  BOOST_CHECK_EQUAL(2, sw.getNumActiveElements());
  BOOST_CHECK_EQUAL(20, sw.getNumDormantElements());
  BOOST_CHECK_EQUAL(1, sw.getIthElement(0).first);
  BOOST_CHECK_EQUAL(15, sw.getIthElement(0).second);
  BOOST_CHECK_EQUAL(2, sw.getIthElement(1).first);
  BOOST_CHECK_EQUAL(5, sw.getIthElement(1).second);
  BOOST_CHECK_THROW(sw.getIthElement(2), std::out_of_range);
  
  for (int i = 0; i < 8; i++) sw.add(3);
  BOOST_CHECK_EQUAL(10, sw.getNumActiveElements());
  BOOST_CHECK_EQUAL(20, sw.getNumDormantElements());
  
  // Should create dormant window number 3
  sw.add(4);
  BOOST_CHECK_EQUAL(1, sw.getNumActiveElements());
  BOOST_CHECK_EQUAL(30, sw.getNumDormantElements());
  BOOST_CHECK_EQUAL(1, sw.getIthElement(0).first);
  BOOST_CHECK_EQUAL(15, sw.getIthElement(0).second);
  BOOST_CHECK_EQUAL(3, sw.getIthElement(1).first);
  BOOST_CHECK_EQUAL(9, sw.getIthElement(1).second);
  BOOST_CHECK_EQUAL(2, sw.getIthElement(2).first);
  BOOST_CHECK_EQUAL(5, sw.getIthElement(2).second);
  BOOST_CHECK_EQUAL(4, sw.getIthElement(3).first);
  BOOST_CHECK_EQUAL(1, sw.getIthElement(3).second);
  BOOST_CHECK_THROW(sw.getIthElement(4), std::out_of_range);
  
  for (int i = 0; i < 9; i++) sw.add(3);
  
  // Should create dormant window number 4
  sw.add(3);
  BOOST_CHECK_EQUAL(1, sw.getNumActiveElements());
  BOOST_CHECK_EQUAL(40, sw.getNumDormantElements());
  BOOST_CHECK_EQUAL(3, sw.getIthElement(0).first);
  BOOST_CHECK_EQUAL(18, sw.getIthElement(0).second);
  BOOST_CHECK_EQUAL(1, sw.getIthElement(1).first);
  BOOST_CHECK_EQUAL(15, sw.getIthElement(1).second);
  BOOST_CHECK_EQUAL(2, sw.getIthElement(2).first);
  BOOST_CHECK_EQUAL(5, sw.getIthElement(2).second);
  BOOST_CHECK_EQUAL(4, sw.getIthElement(3).first);
  BOOST_CHECK_EQUAL(2, sw.getIthElement(3).second);
  BOOST_CHECK_THROW(sw.getIthElement(4), std::out_of_range);
  
  for (int i = 0; i < 9; i++) sw.add(3);
  
  // Should force a dormant window to be deleted
  sw.add(3);
  BOOST_CHECK_EQUAL(1, sw.getNumActiveElements());
  BOOST_CHECK_EQUAL(40, sw.getNumDormantElements());
  BOOST_CHECK_EQUAL(3, sw.getIthElement(0).first);
  BOOST_CHECK_EQUAL(28, sw.getIthElement(0).second);
  BOOST_CHECK_EQUAL(1, sw.getIthElement(1).first);
  BOOST_CHECK_EQUAL(5, sw.getIthElement(1).second);
  BOOST_CHECK_EQUAL(2, sw.getIthElement(2).first);
  BOOST_CHECK_EQUAL(5, sw.getIthElement(2).second);
  BOOST_CHECK_EQUAL(4, sw.getIthElement(3).first);
  BOOST_CHECK_EQUAL(2, sw.getIthElement(3).second);
  BOOST_CHECK_THROW(sw.getIthElement(4), std::out_of_range);
}
