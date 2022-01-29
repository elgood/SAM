#define BOOST_TEST_MAIN TestVastNetflow
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <sam/tuples/VastNetflow.hpp>
#include <sam/tuples/Tuplizer.hpp>

using namespace sam;
using namespace sam::vast_netflow;

typedef VastNetflow TupleType;

void checkCommon(VastNetflow const& netflow) 
{
  BOOST_CHECK_EQUAL(1365582756.384094, std::get<TimeSeconds>(netflow));
  BOOST_CHECK_EQUAL("2013-04-10 08:32:36", std::get<ParseDate>(netflow));
  BOOST_CHECK_EQUAL("20130410083236.384094", std::get<DateTime>(netflow));
  BOOST_CHECK_EQUAL("17", std::get<IpLayerProtocol>(netflow));
  BOOST_CHECK_EQUAL("UDP", std::get<IpLayerProtocolCode>(netflow));
  BOOST_CHECK_EQUAL("172.20.2.18", std::get<SourceIp>(netflow));
  BOOST_CHECK_EQUAL("239.255.255.250", std::get<DestIp>(netflow));
  BOOST_CHECK_EQUAL(29986, std::get<SourcePort>(netflow));
  BOOST_CHECK_EQUAL(1900, std::get<DestPort>(netflow));
  BOOST_CHECK_EQUAL("0", std::get<MoreFragments>(netflow));
  BOOST_CHECK_EQUAL(0, std::get<CountFragments>(netflow));
  BOOST_CHECK_EQUAL(16, std::get<DurationSeconds>(netflow));
  BOOST_CHECK_EQUAL(184, std::get<SrcPayloadBytes>(netflow));
  BOOST_CHECK_EQUAL(73140, std::get<DestPayloadBytes>(netflow));
  BOOST_CHECK_EQUAL(2588, std::get<SrcTotalBytes>(netflow));
  BOOST_CHECK_EQUAL(76064, std::get<DestTotalBytes>(netflow));
  BOOST_CHECK_EQUAL(40, std::get<FirstSeenSrcPacketCount>(netflow));
  BOOST_CHECK_EQUAL(54, std::get<FirstSeenDestPacketCount>(netflow));
  BOOST_CHECK_EQUAL(0, std::get<RecordForceOut>(netflow));
}


BOOST_AUTO_TEST_CASE( test_makeNetflow )
{
  std::string s = "1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,16,184,73140,"
                         "2588,76064,40,54,0";

  VastNetflow netflow = makeVastNetflow(s);
 
   
  checkCommon(netflow);

}

BOOST_AUTO_TEST_CASE( test_tuplizer )
{
  typedef std::tuple<int> LabelType;
  typedef Edge<size_t, LabelType, TupleType> EdgeType;
  typedef TuplizerFunction<EdgeType, MakeVastNetflow> Tuplizer;
  Tuplizer tuplizer;

  std::string s = "1,1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,16,184,73140,"
                         "2588,76064,40,54,0";

  EdgeType edge = tuplizer(0, s);
  checkCommon(edge.tuple);
  BOOST_CHECK_EQUAL(std::get<0>(edge.label), 1);
  BOOST_CHECK_EQUAL(std::tuple_size<decltype(edge.label)>::value, 1);

}





