#ifndef MAX_HPP
#define MAX_HPP

#include <iostream>
#include <map>

#include <sam/AbstractConsumer.hpp>
#include <sam/BaseComputation.hpp>
#include <sam/Features.hpp>
#include <sam/Util.hpp>
#include <sam/FeatureProducer.hpp>
#include <sam/tuples/Edge.hpp>
#include <vector>

namespace sam {

template <typename T, typename EdgeType, 
          size_t valueField, size_t... keyFields>
class Max: public AbstractConsumer<EdgeType>, 
                               public BaseComputation,
                               public FeatureProducer
{
private:

  // The size of the sliding window
  size_t N; 

  // A mapping from keyFields to the associated max. ******************************  
  std::map<std::string, std::shared_ptr<std::vector<T>>> allWindows;

  // Location in the vector
  std::map<std::string, size_t> position;


public:
  /**
   * Constructor.
   * \param N The size of the sliding window
   * \param nodeId The nodeId of the node that is running this operator.
   * \param featureMap The global featureMap that holds the features produced
   *                   by this operator.
   * \param identifier A unique identifier associated with this operator.
   */
  Max(size_t N,
                          size_t nodeId,
                          std::shared_ptr<FeatureMap> featureMap,
                          std::string identifier) :
                          BaseComputation(nodeId, featureMap, identifier) 
                                          
  {
    this->N = N; 
  }


  /**
   * Main method of an operator.  Processes the tuple.
   * \param input The tuple to process.
   */
  bool consume(EdgeType const& edge) 
  {
    this->feedCount++;

    if (this->feedCount % this->metricInterval == 0) {
      std::cout << "NodeId " << this->nodeId << " number of keys " 
                << allWindows.size() << " feedCount " << this->feedCount
                << std::endl;
    }

    // Generates unique key from key fields
    std::string key = generateKey<keyFields...>(edge.tuple);

    // Create an exponential histogram if it doesn't exist for the given key 
    if (allWindows.count(key) == 0) {
      auto eh = std::shared_ptr<std::vector<T>>(N); //**
      allWindows[key] = eh;
      position[key] = 0; // empty vector
    }

    // Update the data structure 
    T value = std::get<valueField>(edge.tuple);
    allWindows[key][position[key]] = value;
    position[key]++;
    if(position[key] >= N)
    {
      position[key] = 0;
    }


    // Getting the current max and providing that to the feature map. 
    T currentMax = *max_element(std::begin(allWindows[key]), std::end(allWindows[key]));  
    SingleFeature feature(currentMax);  

    // Update the freature map with the new feature.  The feature map
    // takes as input the key for this item, the identifier for this operator,
    // and the feature itself.  The key and the identifier together uniquely
    // identify the feature.
    this->featureMap->updateInsert(key, this->identifier, feature);

    this->notifySubscribers(edge.id, currentMax);

    return true;
  }

  void terminate() {}

};

}
#endif
