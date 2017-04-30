#define BOOST_TEST_MAIN TestNetflow
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include "Netflow.hpp"

using namespace sam;

BOOST_AUTO_TEST_CASE( netflow_test )
{
 
  std::string netflowString1 = "1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";

  Netflow netflow = makeNetflow(netflowString1);
  
  BOOST_CHECK_EQUAL(1365582756.384094, std::get<TIME_SECONDS_FIELD>(netflow));
  BOOST_CHECK_EQUAL("2013-04-10 08:32:36", std::get<PARSE_DATE_FIELD>(netflow));
  BOOST_CHECK_EQUAL("20130410083236.384094", 
                    std::get<DATE_TIME_STR_FIELD>(netflow));
  BOOST_CHECK_EQUAL("17", std::get<IP_LAYER_PROTOCOL_FIELD>(netflow));
  BOOST_CHECK_EQUAL("UDP", std::get<IP_LAYER_PROTOCOL_CODE_FIELD>(netflow));
  BOOST_CHECK_EQUAL("172.20.2.18", std::get<SOURCE_IP_FIELD>(netflow));
  BOOST_CHECK_EQUAL("239.255.255.250", std::get<DEST_IP_FIELD>(netflow));

}
