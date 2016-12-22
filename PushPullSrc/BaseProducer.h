/*
 * BaseProducer.h
 *
 *  Created on: Dec 10, 2016
 *      Author: elgood
 */

#ifndef ABSTRACTPRODUCER_H_
#define ABSTRACTPRODUCER_H_

#include <list>
#include <string>

#include "BaseProducer.h"
#include "AbstractConsumer.h"

using std::list;
using std::string;
using std::size_t;

namespace sam {

class BaseProducer {
private:
	/// The list of consumers that consume from output from this producer
	list<AbstractConsumer const *> consumers;

	/// The producer has a queue of strings to send to the consumers
	string* inputQueue;

	/// The length of the inputQueue
	size_t queueLength;

  /// The number of items in the queue
  size_t numItems;

public:
	BaseProducer(int queueLength);
	virtual ~BaseProducer();

	void registerConsumer(AbstractConsumer const * consumer);
  bool deregisterConsumer(AbstractConsumer const * consumer);
  size_t getNumConsumers() const;
  AbstractConsumer const * getConsumer(size_t i);

  /*
	@Override
	public void parallelFeed(String s) {
		inputQueue[numItems] = s;
		numItems++;
		if (numItems >= inputQueue.length) {
			consumers.parallelStream().forEach(consumer ->
			{
				for (int i = 0; i < inputQueue.length; i++) {
					consumer.feed(inputQueue[i]);
				}
			});
			numItems = 0;
		}
	}*/


};

} /* namespace sam */

#endif /* ABSTRACTPRODUCER_H_ */
