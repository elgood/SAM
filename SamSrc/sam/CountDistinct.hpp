#ifndef COUNT_DISTINCT_HPP
#define COUNT_DISTINCT_HPP

/**
 * A preliminary, non-streaming implementation to count distinct items. Not
 * space efficient, uses O(N) space where N is size of sliding window
 */

#include <iostream>
#include <map>

#include <sam/AbstractConsumer.hpp>
#include <sam/BaseComputation.hpp>
#include <sam/Features.hpp>
#include <sam/Util.hpp>
#include <sam/FeatureProducer.hpp>
#include <sam/tuples/Edge.hpp>

namespace sam {

namespace CountDistinctDetails {

template <typename T>
class CountDistinctDataStructure  {
private:
  // Size of sliding window
  size_t N;

  // Sliding window, tracks the last N items
  T* array;

  // Index of the end of sliding window. Inserted item will be
  // written to array[current]
  int current = 0;

  // Counter representing distinct values in the sliding window
  int distinct_count;

  // indicates that distinct_count should be incremented
  bool new_unique;

  // when overwriting a value, only decrement distinct_count if the
  // old value was unique
  int old_value;
  bool old_unique;

public:
  CountDistinctDataStructure(size_t N) {
    this->N = N;
    array = new T[N]();
    for (int i = 0; i < N; i++) {
      // init array to a known placeholder value, so we can ignore later
      // NOTE: I'd like to use NULL/nullptr here, but struggled w/ type warnings
      array[i] = -1;
    }
    distinct_count = 0;
  }

  ~CountDistinctDataStructure(){
    delete[] array;
  }

  /**
   * Sliding window - we don't want to track the last N distinct items,
   *    we want the distinct count over the last N items. Array should
   *    track the last N items, count is only edited if a new distinct
   *    is found or if one is lost
   *
   * Add new value to sliding window. If the value is unique in the window,
   * then increment the counter. Similarly, if a unique value was overwritten
   * then decrement the counter.
   */
  void insert(T item) {
    new_unique = true;
    old_unique = true;

    // save value to be overwritten
    old_value = array[current];
    array[current] = -1;

    for (int i = 0; i < N; i++) {
      // skip placeholder values
      if (i == -1) continue;

      // check if new value is unique
      if (array[i] == item) {
        new_unique = false;

        // if neither are unique, break early
        if (!old_unique) {
          break;
        }
      }

      // check if old value is unique
      if (array[i] == old_value) {
        old_unique = false;

        // if neither are unique, break early
        if (!new_unique) {
          break;
        }
      }
    }

    // write new value to sliding window
    array[current] = item;
    current++;

    // rotate index
    if (current >= N) {
      current = 0;
    }

    // update counter
    if (new_unique) {
      distinct_count++;
    }
    if (old_unique) {
      distinct_count--;
    }
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

    // Create new value if it doesn't exist for the given key
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
