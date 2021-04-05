#ifndef JACCARD_INDEX_HPP
#define JACCARD_INDEX_HPP

/**
 * This is a simple implementation of Jaccard index that is not space
 * efficient (i.e. O(N) where N is the size of the sliding window).
 */

#include <iostream>
#include <map>
#include <set>

#include <boost/lexical_cast.hpp>
#include <sam/AbstractConsumer.hpp>
#include <sam/BaseComputation.hpp>
#include <sam/Features.hpp>
#include <sam/Util.hpp>
#include <sam/FeatureProducer.hpp>
#include <sam/tuples/Edge.hpp>

namespace sam
{

namespace JaccardIndexDetails {

template <typename T>
class JaccardIndexDataStructure {
private:
  size_t N;
  T* array;
  int current = 0;

public:
  JaccardIndexDataStructure(size_t N) {
    // Allocate and initialize the data strcutures
    this->N = N;
    array = new T[N]();
    for (int i = 0; i < N; i++) array[i] = 0;
  }

  ~JaccardIndexDataStructure() {
    delete[] array;
  }

  /**
   * Adds an item, removing an item if need be.
   */
  void insert(T item) {
    array[current] = item;
    std::cout << "array[" << current << "] = " << array[current] << std::endl; // debug
    current++;
    if (current >= N) {
      current = 0;
    }
  }

  double getJaccardIndex() {
    /**
     * Note this does not handle the edge case where the array hasn't been
     * allocated yet, i.e. this function is called before the first edge
     * is consumed. Need to add a check to make sure N has been set, but
     * the below doesn't work...
     */
     try {
       if (N == 0);
     } catch (std::exception e) {
       std::cerr << "getJaccardIndex Caught exception; no edges have been consumed"
                 << std::endl;
       std::cerr << e.what() << std::endl;
       return 0;
     }

    // sets used for calculating the Jaccard index
    std::set<T> setA;
    std::set<T> setB;

    std::cout << "; size_A = " << setA.size();
    std::cout << "; size_B = " << setB.size();
    std::cout << "; N = " << N;
    std::cout << std::endl; // debug


    /**
     *
     * Let number of elements in set A & B be size_A & size_B respectively, and
     * the number of elements in the intersection of sets A & B be size_intAB,
     * then the Jaccard index can be computed by:
     * size_intAB / (size_A + size_B - size_intAB)
     *
     * Ref: (https://en.wikipedia.org/wiki/Jaccard_index)
     */

    /**
     * To create the two sets we walk through array and put all values from
     * first half into setA and the values from the second half into setB.
     *
     * If this is called with an empty array it generates a memory access error;
     * Need to add error handling to catch that case.
     *
     */
    for (int i = 0; i < N; i++) {
      if (i <= ((N/2)-1)) {
        setA.insert(array[i]);
//        std::cout << "setA[" << i << "] = " << array[i] << std::endl; // debug
      }
      else {
        setB.insert(array[i]);
//        std::cout << "setB[" << i << "] = " << array[i] << std::endl; // debug
      }
    }

    double size_A = setA.size();
    double size_B = setB.size();
    std::set<T> intersectAB;

    std::cout << "; size_A = " << setA.size();
    std::cout << "; size_B = " << setB.size();
    std::cout << std::endl; // debug


    // Find the intersection of the two sets
    set_intersection(setA.begin(), setA.end(), setB.begin(), setB.end(),
                     inserter(intersectAB, intersectAB.begin()));
    double size_intAB = intersectAB.size();

    // Compute the Jaccard index
    double jaccardIndex = size_intAB / (size_A + size_B - size_intAB);

    std::cout << "current = " << current << std::endl; // debug
    std::cout << "size_intAB = " << size_intAB;
    std::cout << "; size_A = " << size_A;
    std::cout << "; size_B = " << size_B;
    std::cout << std::endl; // debug

    return jaccardIndex;
  }
};

}


template <typename T, typename EdgeType,
          size_t valueField, size_t... keyFields>
class JaccardIndex: public AbstractConsumer<EdgeType>,
                    public BaseComputation,
                    public FeatureProducer
{
public:
  typedef typename EdgeType::LocalTupleType TupleType;
private:
  size_t N; ///> Size of sliding window
  typedef JaccardIndexDetails::JaccardIndexDataStructure<T> value_t;

  /// Mapping from the key (e.g. an ip field) to the jaccard index
  /// data structure that is keeping track of the values seen.
  std::map<std::string, value_t*> allWindows;

  // Where the most recent item is located in the array.
  size_t top = 0;

public:
  JaccardIndex(size_t N,
               size_t nodeId,
               std::shared_ptr<FeatureMap> featureMap,
               std::string identifier) :
    BaseComputation(nodeId, featureMap, identifier)
  {
    this->N = N;
  }

  ~JaccardIndex()   {
    for (auto p : allWindows) {
      delete p.second;
    }
  }

  bool consume(EdgeType const& edge)
  {
    TupleType tuple = edge.tuple;

    this->feedCount++;
    if (this->feedCount % this->metricInterval == 0) {
      std::cout << "JaccardIndex: NodeId " << this->nodeId << " feedCount "
                << this->feedCount << std::endl;
    }

    // Generates unique key from key fields
    std::string key = generateKey<keyFields...>(tuple);
//    std::cout << "key = " << key << std::endl; // debug
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
      std::cerr << "JaccardIndex::consume Caught exception trying to cast string "
                << "value of " << sValue << std::endl;
      std::cerr << e.what() << std::endl;
      value = 0;
    }

    allWindows[key]->insert(value);

    // Getting the current Jaccard Index and providing that to the featureMap.
    double currentJaccardIndex = allWindows[key]->getJaccardIndex();
    SingleFeature feature(currentJaccardIndex);
    this->featureMap->updateInsert(key, this->identifier, feature);

    notifySubscribers(edge.id, currentJaccardIndex);

    return true;
  }

  double getJaccardIndex(std::string key) {
    return allWindows[key]->getJaccardIndex();
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
