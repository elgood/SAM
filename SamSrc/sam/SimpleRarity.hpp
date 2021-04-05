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
  bloom_parameters parameters; // All bloom filters will receive the same params
  bloom_filter* filters[5];
  int current_bloom_index = 0;
  int bloom_filter_counter = 0;
  const float threshold = .75;  // 75% of bloom filter capacity


public:
  SimpleRarity(size_t N,
            size_t nodeId,
            std::shared_ptr<FeatureMap> featureMap,
            std::string identifier) :
    BaseComputation(nodeId, featureMap, identifier) 
  {

    this->parameters.projected_element_count = 100;
    this->parameters.false_positive_probability = 0.1;
    this->parameters.random_seed = 0xA5A5A5A5;
    this->parameters.compute_optimal_parameters();
    this->N = N; // not really needed.

    for(int i= 0; i < 5; i++) {
      filters[i] = new bloom_filter(this->parameters);
    }

  }

  ~SimpleRarity()   {

  }

  bool consume(EdgeType const& edge) 
  {
    TupleType tuple = edge.tuple;

    this->feedCount++;
    this->bloom_filter_counter++;

    if (this->feedCount % this->metricInterval == 0) {
      std::cout << "SimpleRarity: NodeId " << this->nodeId << " feedCount " 
                << this->feedCount << std::endl;
    }

    // Generates unique key from key fields. Determine proper hash function here to compute bloom filter.
    std::string key = generateKey<keyFields...>(tuple);


    // Get the bloom filter result and provide that to the featureMap.
    bool bloom_filter_result = isRare(key);
   // bool bloom_filter_result = false;
    SingleFeature feature(bloom_filter_result);
    this->featureMap->updateInsert(key, this->identifier, feature);

    notifySubscribers(edge.id, bloom_filter_result);


    std::cout << "Inserting value key to bloom filter " <<  this->current_bloom_index;


    std::cout << "feed count "  << this->feedCount;
    double capacity = (double)this->bloom_filter_counter / (double)this->parameters.projected_element_count;
    std::cout << std::setprecision(10) << "Current capacity" << capacity;

    filters[this->current_bloom_index]->insert(key);

    if (capacity < 0.25) {
      std::cout << "Inserting value key to bloom filter 25";
      insert(1, key);
    }
    else if (capacity < 0.50) {
      std::cout << "Inserting value key to bloom filter 50";
      insert(2, key);
    }
    else if (capacity < 0.75) {
      std::cout << "Inserting value key to bloom filter 75";
      insert(3, key);
    }
    else if (capacity < 1.00) {
      std::cout << "Inserting value key to bloom filter 100";
      insert(4, key);
    }
    else if (capacity >=  1.00) {

     // clear contents of full bloom filter
     filters[this->current_bloom_index]->clear();
     // reset counter
     bloom_filter_counter = 0;

     // move current_bloom_index to next filter
     if (this->current_bloom_index + 1 >= 5) {
         this->current_bloom_index = 0;
     }
     else {
       this->current_bloom_index++;
     }

    }

    return true;
  }

  void insert(int num_of_inserts, std::string key) {

    for (int i=num_of_inserts; i<5; i++) {
    // if index = 3, it'll wrap to 0

      if (this->current_bloom_index + i >= 5) {
        filters[this->current_bloom_index % 5]->insert(key);
       }
       else {
        filters[this->current_bloom_index]->insert(key);
       }
    }
  }

  // return true if key not in bloom filter
  bool isRare(std::string key) {

    if (filters[this->current_bloom_index]->contains(key)) {
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
