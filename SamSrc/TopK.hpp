#ifndef TOPK_HPP
#define TOPK_HPP

#include <vector>
#include <string>
#include <map>

#include "SlidingWindow.hpp"
#include "AbstractConsumer.hpp"
#include "BaseComputation.hpp"
#include "Util.hpp"
#include "FeatureProducer.hpp"

namespace sam {

class TopKException : public std::runtime_error {
public:
  TopKException(char const * message) : std::runtime_error(message) { } 

};

template <typename T, 
          typename TupleType, 
          size_t valueField,
          size_t... keyFields>
class TopK: public AbstractConsumer<TupleType>, 
            public BaseComputation,
            public FeatureProducer
{
private:
  size_t N; ///>Total number of elements
  size_t b; ///>Number of elements per window
  size_t k; ///>Top k elements managed

  std::map<std::string, std::shared_ptr<SlidingWindow<T>>> allWindows; 
  
public:
  TopK(size_t N, size_t b, size_t k,
       size_t nodeId,
       std::shared_ptr<FeatureMap> featureMap,
       string identifier);
     

  bool consume(TupleType const& tuple);

  void terminate() {}
     
};

template <typename T, typename TupleType, size_t valueField, 
          size_t... keyFields>
TopK<T, TupleType, valueField, keyFields...>::TopK(
      size_t N, 
      size_t b, 
      size_t k,
      size_t nodeId,
      std::shared_ptr<FeatureMap> featureMap,
      std::string identifier) :
      BaseComputation(nodeId, featureMap, identifier)
{
  this->N = N;
  this->b = b;
  this->k = k;
}

template <typename T, typename TupleType, size_t valueField,
          size_t... keyFields>
bool TopK<T, TupleType, valueField, keyFields...>::consume(
  TupleType const& tuple) 
{
  //std::cout << "tuple in topk " << toString(tuple) << std::endl;
  this->feedCount++;
  if (this->feedCount % this->metricInterval == 0) {
    std::cout << "NodeId " << this->nodeId << " allWindows.size() " 
              << allWindows.size() << std::endl;
  }

  // Creating a hopefully unique key from the key fields
  std::string key = generateKey<keyFields...>(tuple);
  
  if (allWindows.count(key) == 0) {
    auto sw = std::shared_ptr<SlidingWindow<size_t>>(
                new SlidingWindow<size_t>(N,b,k));
    std::pair<std::string, std::shared_ptr<SlidingWindow<size_t>>> p(key, sw);
    allWindows[key] = sw;    
  }
  
  std::string sValue;
  try { 
    sValue = boost::lexical_cast<std::string>(std::get<valueField>(tuple));
  } catch (std::exception e) {
    throw TopKException(e.what()); 
  }
 
  T value;
  try { 
    value = boost::lexical_cast<T>(sValue);
  } catch (std::exception e) {
    throw TopKException(e.what()); 
  }
  
  auto sw = allWindows[key];
  sw->add(value);

  std::vector<string> keys        = sw->getKeys();
  std::vector<double> frequencies = sw->getFrequencies();
  
  if (keys.size() > 0 && frequencies.size() > 0) {
    //std::cout << "keys.size() " << keys.size() << " frequencies.size() " 
    //          << frequencies.size() << std::endl;
    TopKFeature feature(keys, frequencies);
    //std::cout << "Createad feature " << std::endl;
    //std::cout << "key " << key << " identifier " << this->identifier << std::endl;
    this->featureMap->updateInsert(key, this->identifier, feature);
    //std::cout << "update insert " << std::endl;

    std::size_t id = std::get<0>(tuple);
    // notifySubscribers only takes doubles right now
    //std::cout << "notifying subscribers frequencies" << frequencies[0] 
    //          << " id " << id << "key " << key << std::endl;
    notifySubscribers(id, frequencies[0]);
    //std::cout << "Notifified subscribers " <<std::endl;

  }


  return true;
}




}

#endif
