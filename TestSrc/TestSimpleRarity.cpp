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
    rarity(10, nodeId, featureMap, "rarity");
                                

  string netflowString1 = "1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";
  string netflowString2 = "1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,2,0,1,0,0";

  EdgeType edge1 = tuplizer(1, netflowString1);
  EdgeType edge2 = tuplizer(2, netflowString2);


  rarity.consume(edge1);
  bool is_rare = rarity.isRare("239.255.255.250");

  BOOST_TEST(is_rare == false);

  is_rare = rarity.isRare("xvy");
  BOOST_TEST(is_rare == true);

  is_rare = rarity.isRare("Ti");
  BOOST_TEST(is_rare == true);

  is_rare = rarity.isRare("12");
  BOOST_TEST(is_rare == true);

  is_rare = rarity.isRare("9");
  BOOST_TEST(is_rare == true);



  for (int i = 0; i < 200; i++) rarity.consume(edge1);

  is_rare = rarity.isRare("239.255.255.250");
  BOOST_TEST(is_rare == false);


  //for (int i = 0; i < 9; i++)
  //{
   // sum.consume(edge1);
  //  total = sum.getSum("239.255.255.250");
  //  BOOST_CHECK_EQUAL(total, 10);
 // }

 // sum.consume(edge2);
 // total = sum.getSum("239.255.255.250");
 // BOOST_CHECK_EQUAL(total, 11);

 // sum.consume(edge2);
 // total = sum.getSum("239.255.255.250");
 // BOOST_CHECK_EQUAL(total, 12);
}
