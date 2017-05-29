#define BOOST_TEST_MAIN TestFeatureMap
#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>
#include <stdexcept>
#include <thread>
#include "FeatureMap.hpp"
#include "Features.hpp"

using namespace sam; 


BOOST_AUTO_TEST_CASE( map_test_updateInsert )
{

  int capacity = 1000;
  FeatureMap mymap( capacity );


  BooleanFeature bf1(false);
  mymap.updateInsert("192.168.0.1", "testbooleanfeature", bf1);

  std::shared_ptr<Feature const> bf2 = 
    mymap.at("192.168.0.1", "testbooleanfeature");

  BOOST_CHECK(*bf2.get() == bf1);  

}

BOOST_AUTO_TEST_CASE( map_test_multi_threads )
{
  // The capacity of the map.  It doesn't resize right now.
  int capacity = 1000;

  // The number of inserts to perform per thread
  int numInserts = 10000;

  // The feature map under test
  FeatureMap mymap( capacity );

  // The identifier of the feature that we are inserting.
  std::string featureName = "testbooleanfeature";

  // The number of concurrent threads
  int numThreads = 10;
  std::vector<std::thread> threads;

  
  for (int i = 0; i < numThreads; i++) 
  {
    threads.push_back(std::thread([i, numInserts, featureName, &mymap]() {
      for (int j = 0; j < numInserts; j++) {
        std::string ip = "192.168.0." + boost::lexical_cast<std::string>(i);
        if (i % 2 == 0) {
          BooleanFeature bf(false);  
          mymap.updateInsert(ip, featureName, bf); 
        } else {
          BooleanFeature bf(true);  
          mymap.updateInsert(ip, featureName, bf); 
        }
      }
    }));
  }

  for (int i = 0; i < numThreads; i++) {
    threads[i].join();
  }


  for (int i = 0; i < numThreads; i++) 
  {
    std::string ip = "192.168.0." + boost::lexical_cast<std::string>(i);
    std::shared_ptr<Feature const> bf = mymap.at(ip, featureName);
    if (i % 2 == 0) {
      BooleanFeature expected(false);
      BOOST_CHECK( expected == *bf.get() );
    } else {
      BooleanFeature expected(true);
      BOOST_CHECK( expected == *bf.get() );
    }
  }
}
