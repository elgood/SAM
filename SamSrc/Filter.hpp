#ifndef FILTER_HPP
#define FILTER_HPP

#include <string>
#include "Expression.hpp"
#include "Grammars.hpp"

using std::string;

namespace sam {

template <typename TupleType, size_t... keyFields>
class Filter: public AbstractConsumer<TupleType>, 
              public BaseComputation<0, keyFields...>, 
              public BaseProducer<TupleType>
{
private:
  Expression<FilterGrammar<std::string::const_iterator>> const& expression;
public:
  Filter(Expression<FilterGrammar<std::string::const_iterator>> const& _exp,
         size_t nodeId,
         FeatureMap& featureMap,
         string identifier,
         size_t queueLength) :
         BaseComputation<0, keyFields...>(nodeId, featureMap, identifier), 
         BaseProducer<TupleType>(queueLength),
         expression(_exp)
  {}

  bool consume(TupleType const& t);

};

template <typename TupleType, size_t... keyFields>
bool Filter<TupleType, keyFields...>::consume(TupleType const& t) 
{
  string key = this->generateKey(t);

  try {
    double result = expression.evaluate(key, this->featureMap); 
    BooleanFeature feature(result);
    this->featureMap.updateInsert(key, this->identifier, feature); 
    if ( result ) {
      this->parallelFeed(t);
    }
  } catch (std::exception e) {
    // If there was an exception, it means that some of the necessary 
    // features weren't present, so the boolean feature is false.
    BooleanFeature feature(0);
    this->featureMap.updateInsert(key, this->identifier, feature);  
  }
  return true;
}


}

#endif
