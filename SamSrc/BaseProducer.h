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

#include "BaseProducer.h"
#include "AbstractConsumer.h"

using std::vector;
using std::string;
using std::size_t;

namespace sam {

class BaseProducer {
protected:
	/// The list of consumers that consume from output from this producer
	vector<AbstractConsumer *> consumers;

	/// The producer has a queue of strings to send to the consumers
	string* inputQueue;

	/// The length of the inputQueue
	size_t queueLength;

  /// The number of items in the queue
  size_t numItems;

public:
	BaseProducer(int queueLength);
	virtual ~BaseProducer();

	void registerConsumer(AbstractConsumer * consumer);
  bool deregisterConsumer(AbstractConsumer * consumer);
  size_t getNumConsumers() const;
  AbstractConsumer const * getConsumer(size_t i);

  void parallelFeed(string s);

};

} /* namespace sam */

#endif /* ABSTRACTPRODUCER_H_ */
