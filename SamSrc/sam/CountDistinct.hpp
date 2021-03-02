#ifndef COUNT_DISTINCT_HPP
#define COUNT_DISTINCT_HPP

/**
 * A preliminary, non-streaming implementation to count distinct items. Not
 * space efficient, uses O(N) space where N is size of data (or sliding window,
 * after streaming implementation)
 *
 * TODO: convert to streaming implementation
 */

#include <iostream>
#include <map>

#include <sam/AbstractConsumer.hpp>
#include <sam/BaseComputation.hpp>
#include <sam/ExponentialHistogram.hpp>
#include <sam/Features.hpp>
#include <sam/Util.hpp>
#include <sam/FeatureProducer.hpp>
#include <sam/tuples/Edge.hpp>

namespace sam {

namespace CountDistinctDetails {

template <typename T>
class CountDistinctDataStructure  {
private:
  size_t N;
  T* array;
  T distinct_count;
  int current = 0;

public:
  CountDistinctDataStructure(size_t N) {
    this->N = N;
    array = new T[N]();
    for (int i = 0; i < N; i++) array[i] = 0;
    distinct_count = 0;
  }

  ~CountDistinctDataStructure(){
    delete[] array;
  }

  /**
   * If item exists in array, return early. Otherwise, add item and
   * increment distinct item count.
   *
   * TODO: implement sliding window, remove old items & decrement counter
   */
  void insert(T item) {
    // If item exists in array, return early
    for (int i = 0; i < N; i++) {
      if (array[i] == item) {
        return;
      }
    }
    array[current] = item;
    current++;
    // NOTE: Check is irrelevant until sliding window implementation
    if (current >= N) {
      current = 0;
    }
    distinct_count++;
  }

  T getDistinctCount() {
    return distinct_count;
  }
};

}

template <typename T, typename EdgeType,
          size_t valueField, size_t... keyFields>

class CountDistinct: public AbstractConsumer<EdgeType>,
                     public BaseComputation,
                     public FeatureProducer
{
private:

  // NOTE: In streaming implementation, size of sliding window.
  //       Unused for non-streaming implementation
  // Size of total number of edges (non-streaming implementation)
  size_t N;
  typedef CountDistinctDetails::CountDistinctDataStructure<T> value_t;

  // Mapping from the key to the associated data structure keeping track of unique
  // values seen.
  std::map<std::string, value_t*> allWindows;

public:
  /**
   * Constructor.
   * \param N The number of elements in the sliding window.
   * \param nodeId The nodeId of the node that is running this operator.
   * \param featureMap The global featureMap that holds the features produced
   *                   by this operator.
   * \param identifier A unique identifier associated with this operator.
   */
  CountDistinct(size_t N,
                size_t nodeId,
                std::shared_ptr<FeatureMap> featureMap,
                std::string identifier) :
                BaseComputation(nodeId, featureMap, identifier)

  {
    this->N = N;
  }

  ~CountDistinct() {
    for (auto pair : allWindows) {
      delete pair.second;
    }
  }

  /**
   * Main method of an operator. Processes the tuple.
   * \param input The tuple to process.
   */
  bool consume(EdgeType const& edge)
  {
    this->feedCount++;

    if (this->feedCount % this->metricInterval == 0) {
      std::cout << "CountDistinct: NodeId " << this->nodeId << " number of keys "
                << allWindows.size() << " feedCount " << this->feedCount
                << std::endl;
    }

    // Generates unique key from key fields
    std::string key = generateKey<keyFields...>(edge.tuple);

    // Create an exponential histogram if it doesn't exist for the given key
    if (allWindows.count(key) == 0) {
      auto value = new value_t(N);
      allWindows[key] = value;
    }

    // Update the data structure
    T value = std::get<valueField>(edge.tuple);
    allWindows[key]->insert(value);

    // Get the current distinct item count and provide that to the feature map.
    T currentDistinctCount = allWindows[key]->getDistinctCount();
    SingleFeature feature(currentDistinctCount);

    // Update the freature map with the new feature. The feature map
    // takes as input the key for this item, the identifier for this operator,
    // and the feature itself. The key and the identifier together uniquely
    // identify the feature.
    this->featureMap->updateInsert(key, this->identifier, feature);

    this->notifySubscribers(edge.id, currentDistinctCount);

    return true;
  }

  T getDistinctCount(std::string key) {
    return allWindows[key]->getDistinctCount();
  }

  void terminate() {}

};

}

#endif
