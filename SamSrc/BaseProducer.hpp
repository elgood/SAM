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
	std::vector<std::shared_ptr<AbstractConsumer<T>>> consumers;

	/// The producer has a queue of strings to send to the consumers
	T* inputQueue;

	/// The length of the inputQueue
	size_t queueLength;

  /// The number of items in the queue
  size_t numItems;

public:
  //BaseProducer();
	BaseProducer(int queueLength);
	virtual ~BaseProducer();

  /**
   * Registers a consumer that will consume the output of this producer.
   * \param consumer The object that consumes the output of this producer.
   */
	void registerConsumer(std::shared_ptr<AbstractConsumer<T>> consumer);
  
  bool deregisterConsumer(std::shared_ptr<AbstractConsumer<T>> consumer);
  
  size_t getNumConsumers() const;
  std::shared_ptr<const AbstractConsumer<T>> getConsumer(size_t i);

  void parallelFeed(T const& s);

};

//template <typename T>
//BaseProducer<T>::BaseProducer() : BaseProducer(1) {}

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
void BaseProducer<T>::registerConsumer(
  std::shared_ptr<AbstractConsumer<T>> consumer)
{
	consumers.push_back(consumer);
}

template <typename T>
bool BaseProducer<T>::deregisterConsumer(
  std::shared_ptr<AbstractConsumer<T>> consumer)
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
    //std::cout << "parallelfeed " << numItems << " " << queueLength << std::endl;

    /*boost::thread_group threads;
    for (int i = 0; i < consumers.size(); i++) {
     
      threads.create_thread([this, i]()
        {

          for (int j = 0; j < this->queueLength; j++) {
            this->consumers[i]->consume(inputQueue[j]);
            
          }
        }

      );
      
    }*/
    //REMOVE serial implementation for debugging
    for (int j = 0; j < this->queueLength; j++) {
      for (int i = 0; i < consumers.size(); i++) {
        //std::cout << "i j " << i << " " << j << " blahblahblah" << std::endl;
        this->consumers[i]->consume(inputQueue[j]);
        //std::cout << "after consume" << std::endl;
      }
    } //END REMOVE
    numItems = 0;
    //threads.join_all();

  }
  //std::cout << "exiting parallelfeed" << std::endl;

}


} /* namespace sam */

#endif /* ABSTRACTPRODUCER_H_ */
