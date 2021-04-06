#ifndef SIMPLE_RARITY_HPP
#define SIMPLE_RARITY_HPP

/**
* This is a simple implementation of determining rarity using sliding bloom filters.
* The basic idea is to define a window of bloom filters and populate each filter and begin
* to "slide" to the next filter once the filter has reached capacity. The problem with a fixed
* static filter is that it will eventually saturate and become useless as it will report too many
* false positives. This maintains a window of bloom filters to avoid that.
 *
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
#include <bloom/bloom_filter.hpp>

namespace sam 
{


template <typename T, typename EdgeType,
          size_t valueField, size_t... keyFields>
class SimpleRarity: public AbstractConsumer<EdgeType>, 
                 public BaseComputation,
                 public FeatureProducer
{
public:
  typedef typename EdgeType::LocalTupleType TupleType;



private:
  const static size_t N = 5; // Size of sliding window.  This can be dynmaic and provided in ctor but size of the bloom filter is main driver.
  bloom_parameters parameters; // All bloom N filters will receive the same params.
  bloom_filter* filters[N];
  int current_bloom_index = 0; // track which bloom filter index (0 though N)
  int bloom_filter_counter[N]; // counter to know when to reset a filter

public:
  SimpleRarity(int filter_size,
            size_t nodeId,
            std::shared_ptr<FeatureMap> featureMap,
            std::string identifier) :
    BaseComputation(nodeId, featureMap, identifier) 
  {
    // Setup the filter params here. Each N filter will receive same params.
    parameters.projected_element_count = filter_size;
    parameters.false_positive_probability = 0.1;
    parameters.random_seed = 0xA5A5A5A5;
    parameters.compute_optimal_parameters();


    // Create the window of N bloom filters
    for(int i= 0; i < N; i++) {
      filters[i] = new bloom_filter(this->parameters);
    }
    for(int i= 0; i < N; i++) {
      bloom_filter_counter[i] = 0;
    }


  }

  ~SimpleRarity()   {
      // delete all bloom filters
      for (auto p : filters) {
        delete p;
      }
  }

  bool consume(EdgeType const& edge) 
  {
    TupleType tuple = edge.tuple;

    // feedCount is total count since running.
    this->feedCount++;

    // Generates unique key from key fields. This value is hashed in bloom_filter.hpp
     std::string key = "";
     try {
         key = generateKey<keyFields...>(tuple);
      } catch (std::exception e) {
        std::cerr << "SimpleRarity::consume Caught exception trying to generate key ";
        std::cerr << e.what() << std::endl;
        return false;  // exit if key cannot be generated.
      }

    /* Get the bloom filter result and provide that to the featureMap
    *  And then notify notifySubscribers
    *  If this key is new it'll return true. */
    bool bloom_filter_result = this->isRare(key);
    SingleFeature feature(bloom_filter_result);
    this->featureMap->updateInsert(key, this->identifier, feature);
    notifySubscribers(edge.id, bloom_filter_result);

    // get the current capacity
    double capacity_utilization = (double)bloom_filter_counter[current_bloom_index] / (double)parameters.projected_element_count;

    // Insert to the current indexed bloom filter
  //  filters[current_bloom_index]->insert(key);
  //  bloom_filter_counter[current_bloom_index]++;

     /**
     *   Based on the capacity_utilization percentage determine what bloom filters
     *   need to insert to.  This is needed b/c we do not "slide" to the next
     *   bloom filter that is empty as this would produce many false postives.
     *   When the next bloom filter slides, it'll already be at 75% capacity utilization.
     */


    std::cout << std::setprecision(4) << "Bloom Filter Index " <<  current_bloom_index << " Capacity Utilization: " << capacity_utilization << "\n";

    if (capacity_utilization < 0.25) {
      insert(N - 5, key);
    }
    else if (capacity_utilization < 0.50) {
      insert(N - 4, key);
    }
    else if (capacity_utilization < 0.75) {
      insert(N - 3, key);
    }
    else if (capacity_utilization < 1.00) {
      insert(N - 2, key);
    }
    else if (capacity_utilization >=  1.00) {

     // clear contents of full bloom filter
     std::cout <<   "Bloom Filter Index " << current_bloom_index << " is now full. Clearing contents \n";
     filters[current_bloom_index]->clear();
     // reset counter
     bloom_filter_counter[current_bloom_index] = 0;

     // move current_bloom_index to next filter
     if (current_bloom_index + 1 >= N) {
         current_bloom_index = 0;
     }
     else {
       current_bloom_index++;
     }

    }

    return true;
  }

  void insert(int num_of_inserts, std::string key) {

    for (int i=0; i <= num_of_inserts; i++) {
    // if index = 3, it'll wrap to 0

      if (current_bloom_index + i >= N) {
        int index =  (current_bloom_index + i) % N;
        std::cout <<   "Inserting key " << key << " to bloom filter index " << index << "\n";
        filters[index]->insert(key);
        bloom_filter_counter[index]++;
       }
       else {
        int index =  current_bloom_index + i;
        std::cout <<   "Inserting key " << key << " to bloom filter index " << index << "\n";
        filters[index]->insert(key);
        bloom_filter_counter[index]++;
       }
    }
  }

  // return true if key not in bloom filter
  bool isRare(std::string key) {

    if (filters[current_bloom_index]->contains(key)) {
        return false;
    }
    else { return true; }
  }

  void terminate() {}
};

}


#endif
