/*
 * AbstractConsumer.h
 *
 *  Created on: Dec 10, 2016
 *      Author: elgood
 */

#ifndef ABSTRACTCONSUMER_H_
#define ABSTRACTCONSUMER_H_

#include <string>


using std::string;

namespace sam {

class AbstractConsumer {
protected:
  size_t feedCount = 0;

public:
	AbstractConsumer();
	virtual ~AbstractConsumer();

	virtual bool consume(string s) = 0;

};

} /* namespace sam */

#endif /* ABSTRACTCONSUMER_H_ */
