#ifndef FILTER_HPP
#define FILTER_HPP

#include <string>
#include "FilterExpression.hpp"

using std::string;

namespace sam {

class Filter: public AbstractConsumer<Netflow>, public BaseComputation, 
              public BaseProducer<Netflow>
{
private:
  FilterExpression const& expression;
public:
  Filter(FilterExpression const& _expression,
         vector<size_t> keyFields,
         size_t nodeId,
         FeatureMap& featureMap,
         string identifier,
         size_t queueLength) :
         BaseComputation(keyFields, 0, nodeId, featureMap, identifier), 
         BaseProducer(queueLength),
         expression(_expression)
  {}

  bool consume(Netflow const& netflow);

};

bool Filter::consume(Netflow const& netflow) 
{
  string key = generateKey(netflow);

  try {
    double result = expression.evaluate(key, featureMap); 
    BooleanFeature feature(result);
    featureMap.updateInsert(key, identifier, feature); 
    if ( result ) {
      this->parallelFeed(netflow);
    }
  } catch (std::exception e) {
    // If there was an exception, it means that some of the necessary 
    // features weren't present, so the boolean feature is false.
    BooleanFeature feature(0);
    featureMap.updateInsert(key, identifier, feature);  
  }
  return true;
}


}

#endif
