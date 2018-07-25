#ifndef SAM_TEMPORAL_SET_HPP
#define SAM_TEMPORAL_SET_HPP

#include <cstddef>
#include <mutex>
#include <set>
#include "Util.hpp"

namespace sam {

namespace detail {

template <typename PairType>
class PairCompare
{
  bool operator()(PairType const& lhs, PairType const& rhs)
  {
    return lhs.first < rhs.first;
  }
};

}

/**
 * A thread safe set data structure where the elements can expire, i.e. 
 * they have a limited temporal existence.  
 */
template <typename V, typename TimeType>
class TemporalSet
{
public:
  typedef std::pair<V, TimeType> PairType;
  typedef detail::PairCompare<PairType> Compare;
  typedef std::set<PairType, Compare> SetType;
  typedef std::map<V, TimeType> MapType;

private:


  /// How long a value has to live after being inserted.
  TimeType timeToLive;

  // How many mutex slots in the table.
  size_t tableCapacity;

  // Locks for each table element
  std::mutex* mutexes;

  // An array (size tableCapacity) of sets 
  MapType* sets; 

  /// The hash function to use.
  std::function<size_t(V const&)> hashFunction;

public:

  TemporalSet(size_t tableCapacity, 
              std::function<size_t(V const&)> hashFunction)
  {
    this->hashFunction = hashFunction;
    mutexes = new std::mutex[tableCapacity];
    sets = new MapType[tableCapacity]; 
  }

  ~TemporalSet()
  {
    delete[] mutexes;
    delete[] sets;
  }

  /**
   * Inserts the specified value into the set.  If the value already exists,
   * it updates the time associated with the value.  If it doesn't exists
   * adds the value and associates the specified time.
   *
   * The time specified is assumed to be the current time of the system.  As
   * such, besides inserting the value, we also remove any values from the 
   * set data structure that have expired.
   *
   * \param value The value to insert into the set data structure.
   * \param currentTime The time that the value occurred.
   * \return Returns true if the value was newly inserted, false if the
   *  value already existed and the time was updated.
   */
  bool insert(V const& value, TimeType currentTime) 
  {
    size_t index = hashFunction(value) % tableCapacity;
    mutexes[index].lock();
    

    sets[index].insert(pair);
    
    // iterate from the beginning and delete
    auto 
    mutexes[index].unlock();
  }

};
  
} //end namespace sam



#endif
