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

BOOST_AUTO_TEST_CASE( simple_rarity_test ) {

    Tuplizer tuplizer;
    size_t nodeId = 0;
    int bloom_filter_size = 100;
    // create featureMap
    auto featureMap = std::make_shared<FeatureMap>();

    // create rarity
    SimpleRarity<size_t, EdgeType, SrcTotalBytes, DestIp>
            rarity(bloom_filter_size, nodeId, featureMap, "rarity");

    // create some basic netflow strings
    string netflowString1 = "1365582756.384094,2013-04-10 08:32:36,"
                            "20130410083236.384094,17,UDP,172.20.2.18,"
                            "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";
    string netflowString2 = "1365582756.384094,2013-04-10 08:32:36,"
                            "20130410083236.384094,17,UDP,172.20.2.18,"
                            "239.255.255.250,29986,1900,0,0,0,133,0,2,0,1,0,0";

    // parse the netflows using tuplizer
    EdgeType edge1 = tuplizer(1, netflowString1);
    EdgeType edge2 = tuplizer(2, netflowString2);

    //consume one of the edges
    rarity.consume(edge1);

    // This should be in the bloom filter
    bool is_rare = rarity.isRare("239.255.255.250");
    BOOST_TEST(is_rare == false);

    // This should not be in the bloom filter
    is_rare = rarity.isRare("198.255.255.25");
    BOOST_TEST(is_rare == true);

    // This should not be in the bloom filter
    is_rare = rarity.isRare("199.255.255.25");
    BOOST_TEST(is_rare == true);

    // This should not be in the bloom filter
    is_rare = rarity.isRare("200.255.255.25");
    BOOST_TEST(is_rare == true);


    /*   Generate bunch of random netflows with random dest ip addresses to test the sliding
     *   bloom filters.
     */
    string dest_ip;
    for (int i = 0; i < 1000; i++) {

        int octet_1 = rand() % 255;
        int octet_2 = rand() % 255;
        int octet_3 = rand() % 255;
        int octet_4 = rand() % 255;

        dest_ip = std::to_string(octet_1) + "." + std::to_string(octet_2) + "." + std::to_string(octet_3) + "." +
                  std::to_string(octet_4);

        string netflowStringRandDest = "1365582756.384094,2013-04-10 08:32:36,"
                                       "20130410083236.384094,17,UDP,172.20.2.18," + dest_ip +
                                       ",29986,1900,0,0,0,133,0,2,0,1,0,0";

        EdgeType edge = tuplizer(i, netflowStringRandDest);

        rarity.consume(edge);

    }

    // check the last randon netflow.
    is_rare = rarity.isRare(dest_ip);
    BOOST_TEST(is_rare == false);
}