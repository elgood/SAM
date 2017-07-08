#ifndef SAM_FEATURE_PRODUCER_HPP
#define SAM_FEATURE_PRODUCER_HPP

#include "FeatureSubscriber.hpp"

namespace sam {

class FeatureProducer
{
protected:
  /// The list of feature subscribers that want feature updates from this
  /// feature producer.
  std::vector<std::shared_ptr<FeatureSubscriber>> subscribers;  
  std::vector<std::string> names;

public:
  void registerSubscriber(std::shared_ptr<FeatureSubscriber> subscriber,
                          std::string name);
   
};

void FeatureProducer::registerSubscriber(
  std::shared_ptr<FeatureSubscriber> subscriber,
  std::string name) 
{
  subscriber->addFeature(name);
  subscribers.push_back(subscriber);
  names.push_back(name);
}

}

#endif
