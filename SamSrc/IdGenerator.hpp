#ifndef ID_GENERATOR_HPP
#define ID_GENERATOR_HPP

#include <atomic>

namespace sam {

class AbstractIdGenerator
{
public:
  AbstractIdGenerator() {}

  virtual ~AbstractIdGenerator() {}

  virtual size_t generate() = 0;  
};

/**
 * This class handles generating unique ids.  All it does is increment
 * a local atomic counter.  So the ids should be unique for tuples on a node,
 * but they will not be unique across the cluster, but that shouldn't matter
 * because each node is in charge of generating their own ids for each tuple.
 */
class SimpleIdGenerator: public AbstractIdGenerator
{
private:
  static std::atomic<std::uint32_t> counter;

public:
  SimpleIdGenerator() : AbstractIdGenerator() {}
  ~SimpleIdGenerator() {}

  size_t generate() {
    return counter.fetch_add(1);
  }
};

std::atomic<uint32_t> SimpleIdGenerator::counter(0);

} // end namespace sam

#endif
