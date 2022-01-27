#ifndef COUNT_DISTINCT_HPP
#define COUNT_DISTINCT_HPP

/**
 * A preliminary, non-streaming implementation to count distinct items. Bloom
 * filter implementation inspired from Tim Coleman's add_rarity branch. To
 * implement a sliding window with bloom filters, values are inserted into
 * one of several sub bloom filters. After a period of time, the filters are
 * rotated and emptied. A visual example can be seen here:
 * https://programming.guide/sliding-bloom-filter.html
 */

#include <iostream>
#include <map>

#include <sam/AbstractConsumer.hpp>
#include <sam/BaseComputation.hpp>
#include <sam/Features.hpp>
#include <sam/Util.hpp>
#include <sam/FeatureProducer.hpp>
#include <sam/tuples/Edge.hpp>
#include <bloom/bloom_filter.hpp>

namespace sam {

namespace CountDistinctDetails {

template <typename T>
class CountDistinctDataStructure  {
private:
  // Size of sliding window
  size_t N;

  // Number of sub bloom filters, 5 feels sufficient
  const static short num_filters = 5;

  // Parameters to initialize bloom filters
  bloom_parameters params;

  // Array of sub bloom filters. An element is inserted into a
  // "live" bloom filter, which rotates every N / num_filters
  // insertions. Each time the live filter rotates, the oldest
  // filter is emptied.
  bloom_filter* sub_filters[num_filters];

  // Keep track of number of insertions
  int insertion_count = 0;

  // Rotate live filter every (N / num_filters) insertions
  int rotation_freq;

  // Array to keep track of how many unique values are in each
  // sub bloom filter. The distinct count is the cumulative total
  // of each filter count.
  int filter_counts[num_filters];

  // Index tracking current bloom filter
  short live_filter = 0;

public:
  CountDistinctDataStructure(size_t N) {
    this->N = N;

    // Configure bloom filter params
    params.projected_element_count = N / num_filters;
    params.false_positive_probability = 0.0001;
    params.compute_optimal_parameters();

    // Calculate rotation number
    rotation_freq = N / num_filters;

    for (int i = 0; i < num_filters; i++) {
      sub_filters[i] = new bloom_filter(this->params);
      filter_counts[i] = 0;
    }
  }

  ~CountDistinctDataStructure(){
    for (auto filter : sub_filters) {
      delete filter;
    }
  }

  /**
   * Attempt to insert value to sliding window. If the value is unique
   * between all sub bloom filters, then add it to the live filter.
   * Additionally check if we've hit the rotation frequency, and rotate
   * the live filter
   */
  void insert(T item) {

    // Set to false if we get a bloom filter hit
    bool is_unique = true;

    for (int i = 0; i < num_filters; i++) {
      if (sub_filters[i]->contains(item)) {
        is_unique = false;
        break;
      }
    }

    // If item is unique, add to live filter and increment counter
    if (is_unique) {
      sub_filters[live_filter]->insert(item);
      filter_counts[live_filter]++;
    }

    insertion_count++;

    // Check if it's time to rotate live filters
    if (insertion_count >= rotation_freq) {
      insertion_count = 0;

      live_filter++;

      // wrap live filter index
      if (live_filter >= num_filters) {
        live_filter = 0;
      }

      // clear oldest filter (new live filter)
      sub_filters[live_filter]->clear();
      filter_counts[live_filter] = 0;
    }
  }

  T getDistinctCount() {
    int distinct_count = 0;
    for (int i = 0; i < num_filters; i++) {
      distinct_count += filter_counts[i];
    }
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
