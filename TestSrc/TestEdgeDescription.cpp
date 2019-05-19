#define BOOST_TEST_MAIN TestEdgeDescription
#include <boost/test/unit_test.hpp>
#include <sam/EdgeDescription.hpp>
#include <sam/VastNetflow.hpp>

using namespace sam;

BOOST_AUTO_TEST_CASE( test_edge_unspecified )
{
  /// Tests unspecifiedSource/Target
  EdgeDescription<VastNetflow, TimeSeconds, DurationSeconds> e;
  BOOST_CHECK_EQUAL(e.unspecifiedSource(), true);
  BOOST_CHECK_EQUAL(e.unspecifiedTarget(), true);
  e.source = "192.168.0.1";
  BOOST_CHECK_EQUAL(e.unspecifiedSource(), false);
  BOOST_CHECK_EQUAL(e.unspecifiedTarget(), true);
  e.target = "192.168.0.1";
  BOOST_CHECK_EQUAL(e.unspecifiedSource(), false);
  BOOST_CHECK_EQUAL(e.unspecifiedTarget(), false);
}

BOOST_AUTO_TEST_CASE( test_fix_time_range )
{
  EdgeDescription<VastNetflow, TimeSeconds, DurationSeconds> e;
  double maxOffset = 10.0;

  // Times are not specified, so exception should be thrown.
  BOOST_CHECK_THROW(e.fixTimeRange(maxOffset), EdgeDescriptionException);
  BOOST_CHECK_THROW(e.fixStartTimeRange(maxOffset), EdgeDescriptionException);
  BOOST_CHECK_THROW(e.fixEndTimeRange(maxOffset), EdgeDescriptionException);

  e.startTimeRange.first = 0;
  e.startTimeRange.second = 20.1;
  BOOST_CHECK_THROW(e.fixTimeRange(maxOffset), EdgeDescriptionException);
  BOOST_CHECK_THROW(e.fixStartTimeRange(maxOffset), EdgeDescriptionException);

  e.endTimeRange.first = 0;
  e.endTimeRange.second = 20.1;
  BOOST_CHECK_THROW(e.fixTimeRange(maxOffset), EdgeDescriptionException);
  BOOST_CHECK_THROW(e.fixEndTimeRange(maxOffset), EdgeDescriptionException);

  e.startTimeRange.first = std::numeric_limits<double>::lowest();
  e.startTimeRange.second = 10;
  e.fixStartTimeRange(maxOffset);
  BOOST_CHECK_EQUAL(e.startTimeRange.first, 0);
  BOOST_CHECK_EQUAL(e.startTimeRange.second, 10);

  e.endTimeRange.first = std::numeric_limits<double>::lowest();
  e.endTimeRange.second = 10;
  e.fixEndTimeRange(maxOffset);
  BOOST_CHECK_EQUAL(e.endTimeRange.first, 0);
  BOOST_CHECK_EQUAL(e.endTimeRange.second, 10);

  e.startTimeRange.first = 0; 
  e.startTimeRange.second = std::numeric_limits<double>::max();
  e.fixStartTimeRange(maxOffset);
  BOOST_CHECK_EQUAL(e.startTimeRange.first, 0);
  BOOST_CHECK_EQUAL(e.startTimeRange.second, 10);

  e.endTimeRange.first = 0; 
  e.endTimeRange.second = std::numeric_limits<double>::max();
  e.fixEndTimeRange(maxOffset);
  BOOST_CHECK_EQUAL(e.endTimeRange.first, 0);
  BOOST_CHECK_EQUAL(e.endTimeRange.second, 10);



}


