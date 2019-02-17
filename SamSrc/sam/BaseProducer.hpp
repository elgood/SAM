/**
 * BaseProducer.h
 *
 *  Created on: Dec 10, 2016
 *      Author: elgood
 */

#ifndef ABSTRACTPRODUCER_H_
#define ABSTRACTPRODUCER_H_

#include <vector>
#include <string>
#include <thread>
#include <functional>
#include <mutex>

#include <sam/AbstractConsumer.hpp>
#include <sam/Util.hpp>
//#include <boost/thread/thread.hpp>

namespace sam {

template <typename T>
class BaseProducer {
private:
  // These are the threads we use for parallelizing the parallelFeed
  // function.  
  std::vector<std::thread> threads;

  // The function that each of the threads run in the parallelFeed function.
  std::function<void(size_t)> parallelFeedFunction;

  // Multiple threads access the parallelFeed method.  This mutex prevents
  // problems
  std::mutex lock;

protected:
  /// The list of consumers that consume from output from this producer
  std::vector<std::shared_ptr<AbstractConsumer<T>>> consumers;

  /// The producer has a queue of strings to send to the consumers
  T* inputQueue;

  /// The length of the inputQueue
  size_t queueLength;

  /// The number of items in the queue
  size_t numItems;

  /// The number of items passed to parallelFeed
  size_t numReadItems = 0;

public:
  BaseProducer(size_t queueLength);
  virtual ~BaseProducer();

  /**
   * Registers a consumer that will consume the output of this producer.
   * \param consumer The object that consumes the output of this producer.
   */
  void registerConsumer(std::shared_ptr<AbstractConsumer<T>> consumer);

  /**
   * Removes the consumer from list of consumers. (Note: not implemented)
   */  
  void deregisterConsumer(std::shared_ptr<AbstractConsumer<T>> consumer);
 
  /**
   * Gets the number of consumers that are registered to this class.
   */ 
  size_t getNumConsumers() const;

  /**
   * Gets the ith consumer from the list of consumers.
   */
  std::shared_ptr<const AbstractConsumer<T>> getConsumer(size_t i);

  /**
   * Feeds the provided item to each of the consumers in parallel.
   */
  void parallelFeed(T const& s);

  size_t getNumReadItems() const { return numReadItems; }

};

template <typename T>
BaseProducer<T>::BaseProducer(size_t queueLength) {
  this->queueLength = queueLength;
  inputQueue = new T[queueLength];
  numItems = 0;

  parallelFeedFunction = [this](size_t threadId) {
    for (size_t i = 0; i < this->queueLength; i++) {
      this->consumers[threadId]->consume(this->inputQueue[i]);
    }
  };
}

template <typename T>
BaseProducer<T>::~BaseProducer() {
  delete[] inputQueue;
}

template <typename T>
void BaseProducer<T>::registerConsumer(
  std::shared_ptr<AbstractConsumer<T>> consumer)
{
  consumers.push_back(consumer);
  threads.resize(consumers.size());
}

template <typename T>
void BaseProducer<T>::deregisterConsumer(
  std::shared_ptr<AbstractConsumer<T>> consumer)
{
  throw std::logic_error("BaseProducer::deregisterConsumer is not"
    " implemented");
}

template <typename T>
size_t BaseProducer<T>::getNumConsumers() const { return consumers.size(); }

template <typename T>
void BaseProducer<T>::parallelFeed(T const& item) {
  lock.lock();
  DEBUG_PRINT("BaseProducer::parallelFeed %s numItems %lu queueLength %lu \n", 
    toString(item).c_str(), numItems, queueLength); 
  
  numReadItems++;
  inputQueue[numItems] = item; // T(item);
  numItems++;

  //  this->queueLength));
  if (numItems >= queueLength) {
    DEBUG_PRINT("BaseProducer::parallelFeed %s numItems %lu >= "
      "queueLength %lu consumes.size() %lu \n", toString(item).c_str(), 
      numItems, queueLength, consumers.size()); 
    //for(size_t i = 0; i < consumers.size(); i++) {
    //  threads[i] = std::thread(parallelFeedFunction, i);
    //}

    //for(size_t i = 0; i < consumers.size(); i++) {
    //  threads[i].join();
    //}
    // Serial for debugging
    
    for(size_t i = 0; i < consumers.size(); i++) {
      DEBUG_PRINT("BaseProducer::parallelFeed consumer %lu %s\n", i,
        toString(item).c_str());
      for(size_t j = 0; j < this->queueLength; j++) {
        DEBUG_PRINT("BaseProducer::parallelFeed j %lu queueLength %lu tuple "
          "%s\n", j, this->queueLength, toString(inputQueue[j]).c_str());
        consumers[i]->consume(inputQueue[j]);
      }
    }
    numItems = 0;
  } 

  lock.unlock();


}


} /* namespace sam */

#endif /* ABSTRACTPRODUCER_H_ */
