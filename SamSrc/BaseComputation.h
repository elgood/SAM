#ifndef BASE_COMPUTATION_HPP
#define BASE_COMPUTATION_HPP

#include <vector>
#include <string>

#include "Netflow.h"
#include "FeatureMap.hpp"

using std::vector;
using std::string;

namespace sam
{

class BaseComputation
{
protected:
  size_t metricInterval = 100000;

  vector<size_t> keyFields; ///> Index of fields that are in the key
  size_t valueField;  ///> The target field
  size_t nodeId; ///> Used for debugging/metrics per node

  /// This is a reference to the map that stores the mapping from 
  /// key/featurename to feature.
  FeatureMap& featureMap;

  /// The variable name assigned to this operator.  This is specified
  /// in the query.
  string identifier; 

protected:
  string generateKey(Netflow const & n) const;


public:
  BaseComputation(vector<size_t> keyFields,
                  size_t valueFields,
                  size_t nodeId,
                  FeatureMap& featureMap,
                  string identifier);
  virtual ~BaseComputation() {}

  

};

inline
BaseComputation::BaseComputation(vector<size_t> keyFields,
                                 size_t valueField,
                                 size_t nodeId,
                                 FeatureMap& _featureMap,
                                 string identifier) : 
                                 featureMap(_featureMap)
{
  this->keyFields = keyFields;
  this->valueField = valueField;
  this->nodeId = nodeId;
  this->identifier = identifier;
}

inline
string BaseComputation::generateKey(Netflow const & netflow) const
{
  string key = "";
  for (auto i : keyFields) {
    key = key + netflow.getField(i);
  }
  return key;    
}


}


#endif
