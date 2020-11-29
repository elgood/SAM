#ifndef ID_GENERATOR_HPP
#define ID_GENERATOR_HPP

#include <atomic>
#include <cstddef>

namespace sam {

/*class AbstractIdGenerator
{
public:
  AbstractIdGenerator() {}

  virtual ~AbstractIdGenerator() {}

  virtual size_t generate() = 0;  
};*/

/**
 * This class handles generating unique ids.  All it does is increment
 * a local atomic counter.  So the ids should be unique for tuples on a node,
 * but they will not be unique across the cluster, but that shouldn't matter
 * because each node is in charge of generating their own ids for each tuple.
 *
 * We make this a singleton so anybody that needs to generate an id will
 * be using the same object.
 */
class SimpleIdGenerator
{
private:
  static std::atomic<uint64_t> counter;
  static SimpleIdGenerator* instance;

  SimpleIdGenerator() {}

public:

  static SimpleIdGenerator* getInstance()
  {
    if (!instance) {
      instance = new SimpleIdGenerator();
    }
    return instance;
  }

  size_t generate() {
    return counter.fetch_add(1);
  }
};

std::atomic<uint64_t> SimpleIdGenerator::counter(0);
SimpleIdGenerator* SimpleIdGenerator::instance = 0;

} // end namespace sam

#endif
