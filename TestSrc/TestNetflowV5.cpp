#define BOOST_TEST_MAIN TestVastNetflow
#include <boost/test/unit_test.hpp>
#include <sam/tuples/NetflowV5.hpp>

using namespace sam;
using namespace sam::netflowv5;

//1578588300,24626000,3739416520,192.168.0.1,1,40,3739180654,3739180654,1,2,192.168.0.1,192.168.0.3,0.0.0.0,2305,2305,61811,80,6,0,20,0,0,0,0


BOOST_AUTO_TEST_CASE( test_removeFirstElement )
{
  BOOST_CHECK_EQUAL(1, 1);  
}
