#define BOOST_TEST_MAIN TestActiveWindow
#include <boost/test/unit_test.hpp>
#include "Features.hpp"
#include "FeatureMap.hpp"

using namespace sam;


BOOST_AUTO_TEST_CASE( single_feature )
{
  SingleFeature feature(10.5);
  BOOST_CHECK_EQUAL(feature.evaluate(valueFunc), 10.5);
   
}

BOOST_AUTO_TEST_CASE( map_feature )
{
  FeatureMap featureMap;

  std::string idorig = "beforecollapseid";
  std::string idcollapsed = "collapsedid";
  std::string dest1 = "192.168.0.1";
  std::string src1 = "192.168.0.100";
  std::string src2 = "192.168.0.101";
  double num1 = 12;
  double num2 = 2;
  SingleFeature src1Feature(num1);
  SingleFeature src2Feature(num2);
  int numIter = 10; //Do it a few times for good measure
  for (int i = 0; i < numIter; i++) {
    // Each iteration, update with the same feature, one for src1, one for src2
    
    // In the natural course of things the featuremap will be updated with the
    // original features first.  
    featureMap.updateInsert(dest1 + src1, idorig, src1Feature); 
    featureMap.updateInsert(dest1 + src2, idorig, src2Feature); 

    // The original features exist in the feature map.  Now we look them up.
    std::shared_ptr<const Feature> origFeature1 = 
      featureMap.at(dest1 + src1, idorig); 
    std::map<std::string, std::shared_ptr<Feature>> localFeatureMap1;
    localFeatureMap1[src1] = origFeature1->createCopy();
    MapFeature mapFeature1(localFeatureMap1);
    featureMap.updateInsert(dest1, idcollapsed, mapFeature1);

    std::shared_ptr<const Feature> origFeature2 = 
      featureMap.at(dest1 + src2, idorig); 
    std::map<std::string, std::shared_ptr<Feature>> localFeatureMap2;
    localFeatureMap2[src2] = origFeature2->createCopy();
    MapFeature mapFeature2(localFeatureMap2);
    featureMap.updateInsert(dest1, idcollapsed, mapFeature2);
  }

  // Now we apply the average function and should get back (num1 + num2) /2
  auto aveFunction = [](std::list<std::shared_ptr<Feature>> myList)->double {
    double sum = 0;
    for (auto feature : myList) {
      sum = sum + feature->evaluate(valueFunc);
    }
    return sum / myList.size();
  };

  auto testMapFeature = std::static_pointer_cast<const MapFeature>(
    featureMap.at(dest1, idcollapsed));
  double result = testMapFeature->evaluate(aveFunction);
  BOOST_CHECK_EQUAL(result, (num1 + num2) /2);

  // Adding one more srcip and some different numbers
  std::string src3 = "192.168.0.102";
  num1 = 8;
  num2 = 10;
  double num3 = 22;
  src1Feature = SingleFeature(num1);
  src2Feature = SingleFeature(num2);
  SingleFeature src3Feature(num3);
  featureMap.updateInsert(dest1 + src1, idorig, src1Feature); 
  featureMap.updateInsert(dest1 + src2, idorig, src2Feature); 
  featureMap.updateInsert(dest1 + src3, idorig, src3Feature); 

  // The original features exist in the feature map.  Now we look them up.
  std::shared_ptr<const Feature> origFeature1 = 
    featureMap.at(dest1 + src1, idorig); 
  std::map<std::string, std::shared_ptr<Feature>> localFeatureMap1;
  localFeatureMap1[src1] = origFeature1->createCopy();
  MapFeature mapFeature1(localFeatureMap1);
  featureMap.updateInsert(dest1, idcollapsed, mapFeature1);

  std::shared_ptr<const Feature> origFeature2 = 
    featureMap.at(dest1 + src2, idorig); 
  std::map<std::string, std::shared_ptr<Feature>> localFeatureMap2;
  localFeatureMap2[src2] = origFeature2->createCopy();
  MapFeature mapFeature2(localFeatureMap2);
  featureMap.updateInsert(dest1, idcollapsed, mapFeature2);

  std::shared_ptr<const Feature> origFeature3 = 
    featureMap.at(dest1 + src3, idorig); 
  std::map<std::string, std::shared_ptr<Feature>> localFeatureMap3;
  localFeatureMap3[src3] = origFeature3->createCopy();
  MapFeature mapFeature3(localFeatureMap3);
  featureMap.updateInsert(dest1, idcollapsed, mapFeature3);

  testMapFeature = std::static_pointer_cast<const MapFeature>(
    featureMap.at(dest1, idcollapsed));
  result = testMapFeature->evaluate(aveFunction);
  BOOST_CHECK_EQUAL(result, (num1 + num2 + num3) /3);


}

BOOST_AUTO_TEST_CASE( topk_feature )
{
  std::vector<std::string> keys;
  keys.push_back("1");
  keys.push_back("2");
  std::vector<double> frequencies;
  frequencies.push_back(0.4);
  frequencies.push_back(0.2);
  TopKFeature top2(keys, frequencies);
  
  int index = 0;

  auto topkValueFunction = [index](Feature const * feature)->double
  {
    auto topkFeature = static_cast<TopKFeature const *>(feature);
    return topkFeature->getFrequencies()[0];
  };
  
  BOOST_CHECK_EQUAL(top2.evaluate(topkValueFunction),0.4);
}

BOOST_AUTO_TEST_CASE( test_equality )
{
  BooleanFeature bf1(true);
  BooleanFeature bf2(true);
  BooleanFeature bf3(false);
  BOOST_CHECK(bf1 == bf2);
  BOOST_CHECK(bf1 != bf3);

  SingleFeature sf1(0.5);
  SingleFeature sf2(0.5);
  SingleFeature sf3(0.33);
  BOOST_CHECK(sf1 == sf2);
  BOOST_CHECK(sf1 != sf3);

  std::vector<std::string> keys1;
  keys1.push_back("1");
  keys1.push_back("2");
  std::vector<double> frequencies1;
  frequencies1.push_back(0.4);
  frequencies1.push_back(0.2);
  TopKFeature topk1(keys1, frequencies1);
 
  std::vector<std::string> keys2;
  keys2.push_back("1");
  keys2.push_back("2");
  std::vector<double> frequencies2;
  frequencies2.push_back(0.4);
  frequencies2.push_back(0.2);
  TopKFeature topk2(keys2, frequencies2);
 
  std::vector<std::string> keys3;
  keys3.push_back("1");
  keys3.push_back("2");
  std::vector<double> frequencies3;
  frequencies3.push_back(0.6);
  frequencies3.push_back(0.2);
  TopKFeature topk3(keys3, frequencies3);

  BOOST_CHECK(topk1 == topk2);
  BOOST_CHECK(topk1 != topk3);

  BOOST_CHECK(bf1 != sf1);
  BOOST_CHECK(bf1 != topk1);
  BOOST_CHECK(sf1 != topk1);
 
}
