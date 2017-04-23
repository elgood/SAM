#ifndef BASE_COMPUTATION_HPP
#define BASE_COMPUTATION_HPP

#include <vector>
#include <string>

#include "Tuple.hpp"
#include "FeatureMap.hpp"


namespace sam
{

class BaseComputation
{
protected:
  size_t metricInterval = 100000;

  std::vector<size_t> keyFields; ///> Index of fields that are in the key
  size_t valueField;  ///> The target field
  size_t nodeId; ///> Used for debugging/metrics per node

  /// This is a reference to the map that stores the mapping from 
  /// key/featurename to feature.
  FeatureMap& featureMap;

  /// The variable name assigned to this operator.  This is specified
  /// in the query.
  std::string identifier; 



public:
  BaseComputation(std::vector<size_t> keyFields,
                  size_t valueFields,
                  size_t nodeId,
                  FeatureMap& featureMap,
                  std::string identifier);
  virtual ~BaseComputation() {}

  std::string generateKey(Tuple const & n) const;
};

inline
BaseComputation::BaseComputation(std::vector<size_t> keyFields,
                                 size_t valueField,
                                 size_t nodeId,
                                 FeatureMap& _featureMap,
                                 std::string identifier) : 
                                 featureMap(_featureMap)
{
  this->keyFields = keyFields;
  this->valueField = valueField;
  this->nodeId = nodeId;
  this->identifier = identifier;
}

inline
std::string BaseComputation::generateKey(Tuple const & tuple) const
{
  std::string key = "";
  for (auto i : keyFields) {
    key = key + tuple.getField(i);
  }
  return key;    
}


}


#endif
