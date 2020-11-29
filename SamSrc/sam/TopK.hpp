#ifndef TOPK_HPP
#define TOPK_HPP

#include <vector>
#include <string>
#include <map>

#include <sam/SlidingWindow.hpp>
#include <sam/AbstractConsumer.hpp>
#include <sam/BaseComputation.hpp>
#include <sam/Util.hpp>
#include <sam/FeatureProducer.hpp>

namespace sam {

class TopKException : public std::runtime_error {
public:
  TopKException(char const * message) : std::runtime_error(message) { } 

};

template <typename EdgeType,
          size_t valueField,
          size_t... keyFields>
class TopK: public AbstractConsumer<EdgeType>, 
            public BaseComputation,
            public FeatureProducer
{
public: 
  typedef typename EdgeType::LocalTupleType TupleType;
  typedef typename std::tuple_element<valueField, TupleType>::type ValueType;
private:

  size_t N; ///>Total number of elements
  size_t b; ///>Number of elements per window
  size_t k; ///>Top k elements managed

  std::map<std::string, std::shared_ptr<SlidingWindow<ValueType>>> allWindows; 
  
public:
  /**
   * Constructor
   * \param N Total number of elements in big window.
   * \param b Number of elements in smaller window.
   * \param k Top k elements to be managed.
   * \param nodeId The id of the node running this computation.
   * \param featureMap The FeatureMap object that stores results.
   * \param identifier The identifier for this feature producer.
   */
  TopK(size_t N, size_t b, size_t k,
       size_t nodeId,
       std::shared_ptr<FeatureMap> featureMap,
       string identifier);
     

  bool consume(EdgeType const& edge);

  void terminate() {}
     
};

template <typename EdgeType,
          size_t valueField, size_t... keyFields>
TopK<EdgeType, valueField, keyFields...>::TopK(
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

template <typename EdgeType,
          size_t valueField, size_t... keyFields>
bool TopK<EdgeType, valueField, keyFields...>::consume(
  EdgeType const& edge) 
{
  DEBUG_PRINT("Node %lu TopK::consume %s\n", nodeId, 
              sam::toString(edge.tuple).c_str());
  this->feedCount++;
  if (this->feedCount % this->metricInterval == 0) {
    std::cout << "NodeId " << this->nodeId << " allWindows.size() " 
              << allWindows.size() << std::endl;
  }

  // Creating a hopefully unique key from the key fields
  std::string key = generateKey<keyFields...>(edge.tuple);
 
  // Create a new sliding window if we haven't seen this key before 
  if (allWindows.count(key) == 0) {
    auto sw = std::shared_ptr<SlidingWindow<ValueType>>(
                new SlidingWindow<ValueType>(N,b,k));
    std::pair<std::string, std::shared_ptr<
              SlidingWindow<ValueType>>> p(key, sw);
    allWindows[key] = sw;    
  }
  
  ValueType value = std::get<valueField>(edge.tuple);
  
  auto sw = allWindows[key];
  sw->add(value);

  std::vector<string> keys        = sw->getKeys();
  std::vector<double> frequencies = sw->getFrequencies();
  
  if (keys.size() > 0 && frequencies.size() > 0) {
    TopKFeature feature(keys, frequencies);
    DEBUG_PRINT("Node %lu TopK::consume keys.size() %lu\n",
      nodeId, keys.size());
    this->featureMap->updateInsert(key, this->identifier, feature);

    // notifySubscribers only takes doubles right now
    notifySubscribers(edge.id, frequencies[0]);

  }


  return true;
}




}

#endif
