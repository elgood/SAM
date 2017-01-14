#define BOOST_TEST_MAIN TestExponentialHistogram
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include "ExponentialHistogram.hpp"

using namespace sam;


BOOST_AUTO_TEST_CASE( eh_test_numlevels )
{
  
  BOOST_CHECK_THROW(ExponentialHistogram<size_t>(0,2), std::out_of_range);
  ExponentialHistogram<size_t> eh1(1, 2);
  BOOST_CHECK_EQUAL(eh1.getNumLevels(), 1);
  ExponentialHistogram<size_t> eh2(2, 2);
  BOOST_CHECK_EQUAL(eh2.getNumLevels(), 1);
  ExponentialHistogram<size_t> eh3(3, 2);
  BOOST_CHECK_EQUAL(eh3.getNumLevels(), 1);
  ExponentialHistogram<size_t> eh4(4, 2);
  BOOST_CHECK_EQUAL(eh4.getNumLevels(), 2);
  ExponentialHistogram<size_t> eh5(5, 2);
  BOOST_CHECK_EQUAL(eh5.getNumLevels(), 2);
  ExponentialHistogram<size_t> eh9(9, 2);
  BOOST_CHECK_EQUAL(eh9.getNumLevels(), 2);
  ExponentialHistogram<size_t> eh10(10, 2);
  BOOST_CHECK_EQUAL(eh10.getNumLevels(), 3);
  ExponentialHistogram<size_t> eh21(21, 2); 
  BOOST_CHECK_EQUAL(eh21.getNumLevels(), 3);
  ExponentialHistogram<size_t> eh22(22, 2); 
  BOOST_CHECK_EQUAL(eh22.getNumLevels(), 4);
  BOOST_CHECK_THROW(ExponentialHistogram<size_t>(
                      ExponentialHistogram<size_t>::MAX_SIZE, 2),
                      std::out_of_range);
}

BOOST_AUTO_TEST_CASE( eh_test_add )
{
  ExponentialHistogram<size_t> eh(21, 2);
  BOOST_CHECK_EQUAL(eh.getNumSlots(), 22);
  BOOST_CHECK_EQUAL(eh.getTotal(), 0);
  for(int i = 0; i < 22; i++) {
    eh.add(1);
    BOOST_CHECK_EQUAL(eh.getTotal(), i + 1);
  }
  eh.add(1);
  BOOST_CHECK_EQUAL(eh.getTotal(), 15);
}

BOOST_AUTO_TEST_CASE( eh_test_long_add )
{
  ExponentialHistogram<size_t> eh(12285, 2);
  for(int i =0; i < 1000000000; i++) {
    eh.add(1);
  }
  BOOST_CHECK_CLOSE(static_cast<double>(eh.getTotal()), 
                    static_cast<double>(12285), 
                    static_cast<double>(pow(2,eh.getNumLevels()-1)));

}
