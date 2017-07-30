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
  /**
   * \param subscriber A shared pointer to the feature subscriber
   * \param
   */
  void registerSubscriber(std::shared_ptr<FeatureSubscriber> subscriber,
                          std::string name);

  /**
   * This should be called within the consume method.  Probably there exists
   * a better way to structure this code so that we can ensure it gets
   * called.
   */
  void notifySubscribers(std::size_t id, double value) {
    //std::cout << " Notifying subscriber of value " << value << std::endl;
    //std::cout << "Number of subscribers " << subscribers.size() << std::endl;
    for (int i = 0; i < subscribers.size(); i++) {
      subscribers[i]->update(id, names[i], value);
    }
  }
   
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
