#ifndef FILTER_HPP
#define FILTER_HPP

#include <string>
#include "Expression.hpp"

using std::string;

namespace sam {

template <typename TupleType, size_t... keyFields>
class Filter: public AbstractConsumer<TupleType>, 
              public BaseComputation,
              public BaseProducer<TupleType>
{
private:
  Expression<TupleType> const& expression;
public:
  Filter(Expression<TupleType> const& _exp,
         size_t nodeId,
         FeatureMap& featureMap,
         string identifier,
         size_t queueLength) :
         BaseComputation(nodeId, featureMap, identifier), 
         BaseProducer<TupleType>(queueLength),
         expression(_exp)
  {}

  bool consume(TupleType const& t);

};

template <typename TupleType, size_t... keyFields>
bool Filter<TupleType, keyFields...>::consume(TupleType const& t) 
{
  string key = generateKey<keyFields...>(t);

  double result = 0;
  bool b = expression.evaluate(key, t, result); 
  if (b) {
    BooleanFeature feature(result);
    this->featureMap.updateInsert(key, this->identifier, feature); 
    if ( result ) {
      this->parallelFeed(t);
    } else {
      BooleanFeature feature(0);
      this->featureMap.updateInsert(key, this->identifier, feature);  
    }
  }
  return true;
}


}

#endif
