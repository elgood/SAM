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
#include "sam/bloom_filter.hpp"

namespace sam 
{

namespace SimpleRarityDetails {



class SimpleRarityDataStructure {
private:
  size_t N;

public:
   bloom_parameters  my_parameters;
   bloom_filter filter;

  SimpleRarityDataStructure(size_t N) {

    my_parameters.projected_element_count = 1000;
    my_parameters.false_positive_probability = 0.0001;
    my_parameters.random_seed = 0xA5A5A5A5;
    bloom_filter filter(my_parameters);
    filter.insert("0.0.0.0"); // need to add one item

  }

  ~SimpleRarityDataStructure() {

  }

  /**
   * Adds an item to bloom filter
   */
  void insert(std::string key) {

    filter.insert(key);
  }

  bool isRare(std::string key) {

      if (filter.contains(key)) {
          std::cout << "Bloom filter contains this item";
          return true;
      }
      else { return false; }
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
  bloom_parameters my_parameters;
  bloom_filter filter;

public:
  SimpleRarity(size_t N,
            size_t nodeId,
            std::shared_ptr<FeatureMap> featureMap,
            std::string identifier) :
    BaseComputation(nodeId, featureMap, identifier) 
  {

    this->my_parameters.projected_element_count = 100000;
    this->my_parameters.false_positive_probability = 0.1;
    this->my_parameters.random_seed = 0xA5A5A5A5;
    this->my_parameters.compute_optimal_parameters();
    this->filter = bloom_filter(my_parameters);
    //filter.insert("0.0.0.0"); // need to add one item
    this->N = N; // not really needed.
  }

  ~SimpleRarity()   {

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
   // try {
   std::string key = generateKey<keyFields...>(tuple);

   // } catch (std::exception e) {
   //   std::cerr << "SimpleRarity::consume Caught exception trying to generate key ";
   //  std::cerr << e.what() << std::endl;
   // }


    // Get the bloom filter result and provide that to the featureMap.
    bool bloom_filter_result = isRare(key);
   // bool bloom_filter_result = false;
    SingleFeature feature(bloom_filter_result);
    this->featureMap->updateInsert(key, this->identifier, feature);

    notifySubscribers(edge.id, bloom_filter_result);


  //  try {
    std::cout << "Inserting value key" << key;
    filter.insert(key);

   // } catch (std::exception e) {
   //   std::cerr << "SimpleRarity::consume Caught exception trying to insert key to bloom filter ";
   //   std::cerr << e.what() << std::endl;
   // }

    return true;
  }

  // return true if key not in bloom filter
  bool isRare(std::string key) {

    if (filter.contains(key)) {
        std::cout << "Bloom filter contains this item"
                  << "value of " << key << std::endl;

        return false;
    }
    else { return true; }
  }

  void terminate() {}
};

}


#endif
