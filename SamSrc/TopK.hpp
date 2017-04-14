#ifndef TOPK_HPP
#define TOPK_HPP

#include <vector>
#include <string>
#include <map>

#include "SlidingWindow.hpp"
#include "AbstractConsumer.h"
#include "BaseComputation.h"

namespace sam {

template <typename T>
class TopK: public AbstractConsumer, public BaseComputation
{
private:
  size_t N; ///>Total number of elements
  size_t b; ///>Number of elements per window
  size_t k; ///>Top k elements managed

  std::map<std::string, std::shared_ptr<SlidingWindow<T>>> allWindows; 
  
public:
  TopK(size_t N, size_t b, size_t k,
       vector<size_t> keyFields,
       size_t valueField,
       size_t nodeId,
       FeatureMap& featureMap,
       string identifier);
     

  bool consume(string s);
     
};

template <typename T>
TopK<T>::TopK(size_t N, size_t b, size_t k,
       vector<size_t> keyFields,
       size_t valueField,
       size_t nodeId,
       FeatureMap& featureMap,
       string identifier) :
       BaseComputation(keyFields, valueField, nodeId, featureMap, identifier)
{
  this->N = N;
  this->b = b;
  this->k = k;
}

template <typename T>
bool TopK<T>::consume(string s) 
{
  feedCount++;
  if (feedCount % metricInterval == 0) {
    std::cout << "NodeId " << nodeId << " allWindows.size() " 
              << allWindows.size() << std::endl;
  }
  Netflow netflow(s);

  // Creating a hopefully unique key from the key fields
  string key = generateKey(netflow);
  
  if (allWindows.count(key) == 0) {
    auto sw = std::shared_ptr<SlidingWindow<size_t>>(
                new SlidingWindow<size_t>(N,b,k));
    std::pair<std::string, std::shared_ptr<SlidingWindow<size_t>>> p(key, sw);
    allWindows[key] = sw;    
  }
  
  string sValue = netflow.getField(valueField);
  
  T value = boost::lexical_cast<T>(sValue);
  
  auto sw = allWindows[key];
  sw->add(value);

  vector<string> keys        = sw->getKeys();
  vector<double> frequencies = sw->getFrequencies();
  
  if (keys.size() > 0 && frequencies.size() > 0) {
    TopKFeature feature(keys, frequencies);
    featureMap.updateInsert(key, identifier, feature);
  }

  return true;
}




}

#endif
