/*
 * BaseProducer.cpp
 *
 *  Created on: Dec 10, 2016
 *      Author: elgood
 */

#include "BaseProducer.h"



namespace sam {

BaseProducer::BaseProducer(int queueLength) {
	this->queueLength = queueLength;
	inputQueue = new string[queueLength];
  numItems = 0;
}

BaseProducer::~BaseProducer() {
	delete[] inputQueue;
}

void BaseProducer::registerConsumer(AbstractConsumer const * consumer)
{
	consumers.push_back(consumer);

}

bool BaseProducer::deregisterConsumer(AbstractConsumer const * consumer)
{
  return false;
}

size_t BaseProducer::getNumConsumers() const { return consumers.size(); }


} /* namespace sam */
