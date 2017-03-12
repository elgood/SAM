#ifndef TOPK_HPP
#define TOPK_HPP

#include <vector>
#include <string>
#include <map>

#include "SlidingWindow.hpp"
#include "AbstractConsumer.h"
#include "BaseComputation.h"

using std::string;
using std::map;
using std::vector;
using std::shared_ptr;
using std::cout;
using std::endl;

namespace sam {

/**
 * This encapsulates a feature that is created after analyzing
 * a netflow.  It has two vectors, a list of keys and a list of 
 * frequencies.  
 *
 */
class TopKFeature: public AdditionalFeature 
{
private:
  vector<string> keys;
  vector<double> frequencies;

public:
  TopKFeature(vector<string> keys,
              vector<double> frequencies) 
  {
    this->keys   = keys;
    this->frequencies = frequencies;
  }
  
  string getKey(int i) {
    return keys[i];
  }

  double getFrequency(int i) {
    return frequencies[i];
  }
};

template <typename T>
class TopK: public AbstractConsumer, public BaseComputation
{
private:
  size_t N; ///>Total number of elements
  size_t b; ///>Number of elements per window
  size_t k; ///>Top k elements managed

  map<string, shared_ptr<SlidingWindow<T>>> allWindows; 
  

public:
  TopK(size_t N, size_t b, size_t k,
       vector<size_t> keyFields,
       size_t valueField,
       size_t nodeId,
       ImuxData& imuxData,
       string identifier);
     

  bool consume(string s);

     
};

template <typename T>
TopK<T>::TopK(size_t N, size_t b, size_t k,
       vector<size_t> keyFields,
       size_t valueField,
       size_t nodeId,
       ImuxData& imuxData,
       string identifier) :
       BaseComputation(keyFields, valueField, nodeId, imuxData, identifier)
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
    cout << "NodeId " << nodeId << " number of keys " 
              << allWindows.size() << endl;
  }
  Netflow netflow(s);

  // Creating a hopefully unique key from the key fields
  string key = generateKey(netflow);
  
  if (allWindows.count(key) == 0) {
    auto sw = shared_ptr<SlidingWindow<size_t>>(
                new SlidingWindow<size_t>(N,b,k));
    std::pair<string, shared_ptr<SlidingWindow<size_t>>> p(key, sw);
    allWindows.insert(p);    
  }
  
  string sValue = netflow.getField(valueField);
  
  T value = boost::lexical_cast<T>(sValue);
  
  auto sw = allWindows[key];
  sw->add(value);

  vector<string> keys        = sw->getKeys();
  vector<double> frequencies = sw->getFrequencies();

  auto feature = shared_ptr<TopKFeature>(
                  new TopKFeature(keys, frequencies));
                                                         
  imuxData.addFeature(key, identifier, feature);

  return true;
}




}

#endif
