/*
 * BaseProducer.h
 *
 *  Created on: Dec 10, 2016
 *      Author: elgood
 */

#ifndef ABSTRACTPRODUCER_H_
#define ABSTRACTPRODUCER_H_

#include <vector>
#include <string>

#include "AbstractConsumer.hpp"
#include <boost/thread/thread.hpp>

namespace sam {

template <typename T>
class BaseProducer {
protected:
	/// The list of consumers that consume from output from this producer
	std::vector<AbstractConsumer<T> *> consumers;

	/// The producer has a queue of strings to send to the consumers
	T* inputQueue;

	/// The length of the inputQueue
	size_t queueLength;

  /// The number of items in the queue
  size_t numItems;

public:
	BaseProducer(int queueLength);
	virtual ~BaseProducer();

	void registerConsumer(AbstractConsumer<T> * consumer);
  bool deregisterConsumer(AbstractConsumer<T> * consumer);
  size_t getNumConsumers() const;
  AbstractConsumer<T> const * getConsumer(size_t i);

  void parallelFeed(T const& s);

};

template <typename T>
BaseProducer<T>::BaseProducer(int queueLength) {
	this->queueLength = queueLength;
	inputQueue = new T[queueLength];
  numItems = 0;
}

template <typename T>
BaseProducer<T>::~BaseProducer() {
	delete[] inputQueue;
}

template <typename T>
void BaseProducer<T>::registerConsumer(AbstractConsumer<T> * consumer)
{
	consumers.push_back(consumer);

}

template <typename T>
bool BaseProducer<T>::deregisterConsumer(AbstractConsumer<T> * consumer)
{
  return false;
}

template <typename T>
size_t BaseProducer<T>::getNumConsumers() const { return consumers.size(); }

template <typename T>
void BaseProducer<T>::parallelFeed(T const& item) {
  
  inputQueue[numItems] = T(item);
  numItems++;
  if (numItems >= queueLength) {

    boost::thread_group threads;
    for (int i = 0; i < consumers.size(); i++) {
     
      threads.create_thread([this, i]()
        {

          for (int j = 0; j < this->queueLength; j++) {
            this->consumers[i]->consume(inputQueue[j]);
            
          }
        }

      );
      
    }
    numItems = 0;
    threads.join_all();

  }

}


} /* namespace sam */

#endif /* ABSTRACTPRODUCER_H_ */
