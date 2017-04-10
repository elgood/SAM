/*
 * BaseProducer.cpp
 *
 *  Created on: Dec 10, 2016
 *      Author: elgood
 */

#include "BaseProducer.h"


#include <boost/thread/thread.hpp>

using std::cout;
using std::endl;

namespace sam {

BaseProducer::BaseProducer(int queueLength) {
	this->queueLength = queueLength;
	inputQueue = new string[queueLength];
  numItems = 0;
}

BaseProducer::~BaseProducer() {
	delete[] inputQueue;
}

void BaseProducer::registerConsumer(AbstractConsumer * consumer)
{
	consumers.push_back(consumer);

}

bool BaseProducer::deregisterConsumer(AbstractConsumer * consumer)
{
  return false;
}

size_t BaseProducer::getNumConsumers() const { return consumers.size(); }

void BaseProducer::parallelFeed(string s) {
  inputQueue[numItems] = s;
  numItems++;
  if (numItems >= queueLength) {

    boost::thread_group threads;
    for (int i = 0; i < consumers.size(); i++) {
     
      threads.create_thread([this, i, s]()
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
