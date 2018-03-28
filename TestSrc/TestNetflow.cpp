#define BOOST_TEST_MAIN TestNetflow
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include "Netflow.hpp"

using namespace sam;

void checkCommon(Netflow const& netflow) 
{
  BOOST_CHECK_EQUAL(1365582756.384094, std::get<TimeSeconds>(netflow));
  BOOST_CHECK_EQUAL("2013-04-10 08:32:36", std::get<ParseDate>(netflow));
  BOOST_CHECK_EQUAL("20130410083236.384094", 
                    std::get<DateTime>(netflow));
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

BOOST_AUTO_TEST_CASE( test_removeFirstElement )
{

  std::string before = "45,1,1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";
  std::string expectedAfter = "1,1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";

  std::string after = removeFirstElement(before);
  BOOST_CHECK_EQUAL(after, expectedAfter);  

}

BOOST_AUTO_TEST_CASE( test_getFirstElement )
{

  std::string s = "45,1,1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";

  std::string element = getFirstElement(s);
  BOOST_CHECK_EQUAL(element, "45");  
}


BOOST_AUTO_TEST_CASE( test_makeNetflow )
{
  // Fully specified string, so should work.
  std::string s = "45,1,1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,16,184,73140,"
                         "2588,76064,40,54,0";

  Netflow netflow = makeNetflow(s);
  BOOST_CHECK_EQUAL(45, std::get<SamGeneratedId>(netflow));
  BOOST_CHECK_EQUAL(1, std::get<SamLabel>(netflow));
  checkCommon(netflow);

}

BOOST_AUTO_TEST_CASE( test_makeNetflowWithoutLabel )
{
 
  std::string s = "1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,16,184,73140,"
                         "2588,76064,40,54,0";

  int generatedId = 25;
  Netflow netflow = makeNetflowWithoutLabel(generatedId, DEFAULT_LABEL, s);
 
  BOOST_CHECK_EQUAL(generatedId, std::get<SamGeneratedId>(netflow));
  BOOST_CHECK_EQUAL(DEFAULT_LABEL, std::get<SamLabel>(netflow));
  checkCommon(netflow);

}

BOOST_AUTO_TEST_CASE( test_makeNetflowWithLabel )
{
  std::string s = "1,1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,16,184,73140,"
                         "2588,76064,40,54,0";

  int generatedId = 25;
  Netflow netflow = makeNetflowWithLabel(generatedId, s);
  
  BOOST_CHECK_EQUAL(1, std::get<SamLabel>(netflow)); 
  BOOST_CHECK_EQUAL(generatedId, std::get<SamGeneratedId>(netflow));
  checkCommon(netflow);
}

BOOST_AUTO_TEST_CASE( test_makeNetflow_noLabel )
{
  std::string s = "1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,16,184,73140,"
                         "2588,76064,40,54,0";

  int generatedId = 25;
  Netflow netflow = makeNetflow(generatedId, s);
 
   
  BOOST_CHECK_EQUAL(DEFAULT_LABEL, std::get<SamLabel>(netflow)); 
  BOOST_CHECK_EQUAL(generatedId, std::get<SamGeneratedId>(netflow));
  checkCommon(netflow);

}

BOOST_AUTO_TEST_CASE( test_makeNetflow_withLabel )
{
  std::string s = "1,1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,16,184,73140,"
                         "2588,76064,40,54,0";

  int generatedId = 25;
  Netflow netflow = makeNetflow(generatedId, s);
 
   
  BOOST_CHECK_EQUAL(1, std::get<SamLabel>(netflow)); 
  BOOST_CHECK_EQUAL(generatedId, std::get<SamGeneratedId>(netflow));
  checkCommon(netflow);

}




