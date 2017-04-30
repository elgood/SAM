#ifndef BASE_COMPUTATION_HPP
#define BASE_COMPUTATION_HPP

#include <vector>
#include <string>

#include "FeatureMap.hpp"


namespace sam
{

template <size_t... keyFields>
class BaseComputation
{
protected:
  size_t metricInterval = 100000; ///> How often (per items) to print metrics
  size_t nodeId; ///> Used for debugging/metrics per node

  /// The variable name assigned to this operator.  This is specified
  /// in the query.
  std::string identifier;

  /// This is a reference to the map that stores the mapping from 
  /// key/featurename to feature.
  FeatureMap& featureMap;

public:
  BaseComputation(size_t nodeId,
                  FeatureMap& _featureMap, 
                  std::string identifier)
  : featureMap(_featureMap) 
  {
    this->nodeId = nodeId;
    this->identifier = identifier;
  }


  template<typename... Ts>
  std::string generateKey(std::tuple<Ts...> const& t) const {
    return "";
  }
};

template <size_t valueField, size_t keyField, size_t... keyFields>
class BaseComputation<valueField, keyField, keyFields...> 
  : public BaseComputation<keyFields...>
{
public:

  BaseComputation(size_t nodeId, 
                  FeatureMap& featureMap, 
                  std::string identifier) 
  : BaseComputation<keyFields...>(nodeId, featureMap, identifier)
  {

  }

  template<typename... Ts>
  std::string generateKey(std::tuple<Ts...> const& t) const
  {
    std::string key = boost::lexical_cast<std::string>(std::get<keyField>(t));
    return key + BaseComputation<keyFields...>::generateKey(t);
  }
};


/*
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

  template<typename... Tp>
  std::string generateKey(std::tuple<Tp...> const & t) const;
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

template<int I = 0, typename... Tp>
inline typename std::enable_if<I ==
blah(std::tuple<Tp...> const&, std::vector<size_t> const&)
{

}

template<typename... Tp>
std::string BaseComputation::generateKey(std::tuple<Tp...>Tuple const& tuple) 
const
{
  std::string key = "";
  for (auto i : keyFields) {
    key = key + tuple.getField(i);
  }
  return key;    
}
*/

}


#endif
