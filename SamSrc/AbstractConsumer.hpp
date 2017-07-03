/*
 * AbstractConsumer.h
 *
 *  Created on: Dec 10, 2016
 *      Author: elgood
 */

#ifndef ABSTRACTCONSUMER_HPP
#define ABSTRACTCONSUMER_HPP

#include <string>

namespace sam {

template <typename T>
class AbstractConsumer {
protected:
  size_t feedCount = 0;

public:
	AbstractConsumer() {}
	virtual ~AbstractConsumer() {}

	virtual bool consume(T const& item) = 0;

};



} /* namespace sam */

#endif /* ABSTRACTCONSUMER_H_ */
