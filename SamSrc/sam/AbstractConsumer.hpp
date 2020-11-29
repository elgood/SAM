/**
 * AbstractConsumer.hpp
 *
 *  Created on: Dec 10, 2016
 *      Author: elgood
 */

#ifndef ABSTRACTCONSUMER_HPP
#define ABSTRACTCONSUMER_HPP

#include <string>
#include <sam/tuples/Edge.hpp>

namespace sam {

template <typename EdgeType>
class AbstractConsumer {
protected:
  size_t feedCount = 0;

public:
	AbstractConsumer() {}
	virtual ~AbstractConsumer() {}

	virtual bool consume(EdgeType const& edge) = 0;

  virtual void terminate() = 0;

};



} /* namespace sam */

#endif /* ABSTRACTCONSUMER_H_ */
