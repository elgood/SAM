#ifndef SIMPLE_RARITY_HPP
#define SIMPLE_RARITY_HPP

/**
 * This is a simple implementation of determining rarity using bloom filters. 
 */

#include <iostream>
#include <map>
#include <boost/lexical_cast.hpp>
#include <sam/AbstractConsumer.hpp>
#include <sam/BaseComputation.hpp>
#include <sam/Features.hpp>
#include <sam/Util.hpp>
#include <sam/FeatureProducer.hpp>
#include <sam/tuples/Edge.hpp>

namespace sam 
{

namespace SimpleRarityDetails {

template <typename T>
class SimpleRarityDataStructure {
private:
  size_t N;
  T* array;
  T rare_item;
  int current = 0;

public:
  SimpleRarityDataStructure(size_t N) {
    this->N = N;
    array = new T[N]();
    for (int i = 0; i < N; i++) array[i] = 0;
    total = 0;
  }

  ~SimpleRarityDataStructure() {
    delete[] array;
  }

  /**
   * Adds an item, removing an item if needbe.
   */
  void insert(T item) {

    //TODO Add to bloom filter here? 

    array[current] = item;
    current++;
    if (current >= N) {
      current = 0;
    }
  }

  T getRarity()) {

    // TODO  Keep the rarest feature stored here. 
    return rare_item;
  }
};

}


template <typename T, typename EdgeType,
          size_t valueField, size_t... keyFields>
class SimpleRarity: public AbstractConsumer<EdgeType>, 
                 public BaseComputation,
                 public FeatureProducer
{
public:
  typedef typename EdgeType::LocalTupleType TupleType;
private:
  size_t N; ///> Size of sliding window
  typedef SimpleRarityDetails::SimpleRarityDataStructure<T> value_t;

  /// Mapping from the key (e.g. an ip field) to the rarity  
  /// data structure that is keeping track of the values seen and determining rarity. 
  std::map<std::string, value_t*> allWindows; 
  
  // Where the most recent item is located in the array.
  size_t top = 0;
  
public:
  SimpleRarity(size_t N,
            size_t nodeId,
            std::shared_ptr<FeatureMap> featureMap,
            std::string identifier) :
    BaseComputation(nodeId, featureMap, identifier) 
  {
    this->N = N;
  }

  ~SimpleRarity()   {
    for (auto p : allWindows) {
      delete p.second;
    }
  }

  bool consume(EdgeType const& edge) 
  {
    TupleType tuple = edge.tuple;

    this->feedCount++;
    if (this->feedCount % this->metricInterval == 0) {
      std::cout << "SimpleRarity: NodeId " << this->nodeId << " feedCount " 
                << this->feedCount << std::endl;
    }

    // Generates unique key from key fields. Determine proper hash function here to compute bloom filter.  
    std::string key = generateKey<keyFields...>(tuple);
    if (allWindows.count(key) == 0) {
      auto value = new value_t(N); 
      allWindows[key] = value;
    }

    std::string sValue = 
      boost::lexical_cast<std::string>(std::get<valueField>(tuple));
    T value;
    try {
      value = boost::lexical_cast<T>(sValue);
    } catch (std::exception e) {
      std::cerr << "SimpleRarity::consume Caught exception trying to cast string "
                << "value of " << sValue << std::endl;
      std::cerr << e.what() << std::endl;
      value = 0;
    }

    allWindows[key]->insert(value);
    
    // Get the bloom filter result and provide that to the featureMap.
    T bloom_filter_result = allWindows[key]->getRarity();
    SingleFeature feature(bloom_filter_result);
    this->featureMap->updateInsert(key, this->identifier, feature);

    notifySubscribers(edge.id, bloom_filter_result);

    return true;
  }

  T getRarity(std::string key) {
    return allWindows[key]->getRarity();
  }

  std::vector<std::string> keys() const {
    std::vector<std::string> theKeys;
    for (auto p : allWindows) {
      theKeys.push_back(p.first);
    }
    return theKeys;
  }

  void terminate() {}
};

}


#endif
