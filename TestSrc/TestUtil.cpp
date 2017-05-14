#define BOOST_TEST_MAIN TestUtil
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <tuple>
#include <string>
#include "Util.hpp"
#include "Netflow.hpp"

using namespace sam;


BOOST_AUTO_TEST_CASE( test_transfer_keys )
{

  std::string netflowString1 = "1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";

  Netflow netflow = makeNetflow(netflowString1);

  typedef std::tuple<std::string, int> OutputType;
  OutputType outTuple;

  std::index_sequence<SOURCE_IP_FIELD, SRC_TOTAL_BYTES> sequence;
  outTuple = subtuple(netflow, sequence);
  

  BOOST_CHECK_EQUAL(std::get<SOURCE_IP_FIELD>(netflow), 
                    std::get<0>(outTuple));
  BOOST_CHECK_EQUAL(std::get<SRC_TOTAL_BYTES>(netflow), 
                    std::get<1>(outTuple));


  
} 
