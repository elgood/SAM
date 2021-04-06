#define BOOST_TEST_MAIN TestSimpleSum
#include <boost/test/unit_test.hpp>
#include <sam/SimpleRarity.hpp>
#include <sam/tuples/VastNetflow.hpp>
#include <sam/tuples/Edge.hpp>
#include <sam/tuples/Tuplizer.hpp>

using namespace sam;
using namespace sam::vast_netflow;
using std::string;

typedef VastNetflow TupleType;
typedef EmptyLabel LabelType;
typedef Edge<size_t, LabelType, TupleType> EdgeType;
typedef TuplizerFunction<EdgeType, MakeVastNetflow> Tuplizer; 

BOOST_AUTO_TEST_CASE( simple_rarity_test )
{
  Tuplizer tuplizer;

  std::vector<size_t> keyFields;
  size_t nodeId = 0;
  auto featureMap = std::make_shared<FeatureMap>();
  SimpleRarity<size_t, EdgeType, SrcTotalBytes, DestIp>
    rarity(5, nodeId, featureMap, "rarity");



  string netflowString1 = "1365582756.384094,2013-04-10 08:32:36,"
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";
  string netflowString2 = "1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,2,0,1,0,0";

  EdgeType edge1 = tuplizer(1, netflowString1);
  EdgeType edge2 = tuplizer(2, netflowString2);


  rarity.consume(edge1);

  // netflowString1 & netflowString2 contain dest 239.255.255.250
  bool is_rare = rarity.isRare("239.255.255.250");
  BOOST_TEST(is_rare == false);


  is_rare = rarity.isRare("198.255.255.25");
  BOOST_TEST(is_rare == true);

  is_rare = rarity.isRare("199.255.255.25");
  BOOST_TEST(is_rare == true);

  is_rare = rarity.isRare("200.255.255.25");
  BOOST_TEST(is_rare == true);


  string dest_ip;
  for (int i = 0; i < 1000; i++) {

      int octet_1 = rand() % 255;
      int octet_2 = rand() % 255;
      int octet_3 = rand() % 255;
      int octet_4 = rand() % 255;

      dest_ip = std::to_string(octet_1) + "." + std::to_string(octet_2) + "." + std::to_string(octet_3) + "." + std::to_string(octet_4);

      string netflowStringRandDest = "1365582756.384094,2013-04-10 08:32:36,"
                                     "20130410083236.384094,17,UDP,172.20.2.18," + dest_ip + ",29986,1900,0,0,0,133,0,2,0,1,0,0";

      EdgeType edge = tuplizer(i, netflowStringRandDest);

      rarity.consume(edge);

  }

   // check the last randon netflow.
  is_rare = rarity.isRare(dest_ip);
  BOOST_TEST(is_rare == false);
