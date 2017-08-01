#ifndef BASE_COMPUTATION_HPP
#define BASE_COMPUTATION_HPP

#include <vector>
#include <string>

#include "FeatureMap.hpp"


namespace sam
{

//template <size_t... keyFields>
class BaseComputation
{
protected:
  size_t metricInterval = 100000; ///> How often (per items) to print metrics
  size_t nodeId; ///> Used for debugging/metrics per node

  /// The variable name assigned to this operator.  This is specified
  /// in the query.
  std::string identifier;

  /// This is a pointer to the map that stores the mapping from 
  /// key/featurename to feature.
  std::shared_ptr<FeatureMap> featureMap;

public:
  BaseComputation(size_t nodeId,
                  std::shared_ptr<FeatureMap> featureMap, 
                  std::string identifier)
  {
    this->featureMap = featureMap;
    this->nodeId = nodeId;
    this->identifier = identifier;
  }


};


}


#endif
