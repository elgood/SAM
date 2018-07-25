#define BOOST_TEST_MAIN TestTemporalSet

#include "TemporalSet.hpp"
#include <boost/test/unit_test.hpp>


using namespace sam;

BOOST_AUTO_TEST_CASE( test_temporal_set )
{
  typedef TemporalSet<size_t, double> SetType;

  size_t tableSize = 1000;
  
  UnsignedIntHashFunction hash; 

  SetType set(tableSize, hash);
}

