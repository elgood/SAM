#define BOOST_TEST_MAIN TestEdgeDescription
#include <boost/test/unit_test.hpp>
#include "EdgeDescription.hpp"
#include "Netflow.hpp"

using namespace sam;

BOOST_AUTO_TEST_CASE( test_edge_description )
{
  /// Tests unspecifiedSource/Target
  EdgeDescription<Netflow> e;
  BOOST_CHECK_EQUAL(e.unspecifiedSource(), true);
  BOOST_CHECK_EQUAL(e.unspecifiedTarget(), true);
  e.source = "192.168.0.1";
  BOOST_CHECK_EQUAL(e.unspecifiedSource(), false);
  BOOST_CHECK_EQUAL(e.unspecifiedTarget(), true);
  e.target = "192.168.0.1";
  BOOST_CHECK_EQUAL(e.unspecifiedSource(), false);
  BOOST_CHECK_EQUAL(e.unspecifiedTarget(), false);
}


