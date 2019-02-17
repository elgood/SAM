#ifndef SAM_TEMPORAL_SET_HPP
#define SAM_TEMPORAL_SET_HPP

#include <cstddef>
#include <mutex>
#include <map>
#include <list>
#include <sam/Util.hpp>

namespace sam {

class TemporalSetException : public std::runtime_error {
public:
  TemporalSetException(char const * message) : std::runtime_error(message) { } 
  TemporalSetException(std::string message) : std::runtime_error(message) { } 
};

/**
 * A thread safe set data structure where the elements can expire, i.e. 
 * they have a limited temporal existence.  
 */
template <typename K, typename TimeType>
class TemporalSet
{
public:
  typedef std::pair<K, TimeType> PairType;
  typedef std::list<PairType> ListType;
  typedef std::map<K, TimeType> MapType;

private:

  /// How long a key has to live after being inserted.
  TimeType timeToLive;

  // How many mutex slots in the table.
  size_t tableCapacity;

  // Locks for each table element
  std::mutex* mutexes;

  // An array (size tableCapacity) of maps.  The mapping is from keys to the
  // time that the key occurred. 
  MapType* hashTables; 

  // This keeps the keys in temporal order so we can delete them from
  // the hash table without iterating over the whole hash table.
  ListType* lists;

  /// The hash function to use.
  std::function<size_t(K const&)> hashFunction;

public:

  TemporalSet()
  {
    tableCapacity = 0;
    hashFunction = 0;
    timeToLive = 0;
  }

  TemporalSet(size_t tableCapacity, 
              std::function<size_t(K const&)> hashFunction,
              TimeType timeToLive)
  {
    this->tableCapacity = tableCapacity;
    this->hashFunction  = hashFunction;
    this->timeToLive    = timeToLive;
    mutexes    = new std::mutex[tableCapacity];
    hashTables = new MapType[tableCapacity]; 
    lists      = new ListType[tableCapacity];
  }

  ~TemporalSet()
  {
    delete[] mutexes;
    delete[] lists;
    delete[] hashTables;
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
   * \param key The key to insert into the set data structure.
   * \param currentTime The time that the value occurred.
   * \return Returns true if the value was newly inserted, false if the
   *  value already existed and the time was updated.
   */
  bool insert(K const& key, TimeType currentTime) 
  {

    PairType pair(key, currentTime);
    size_t index = hashFunction(key) % tableCapacity;

    // Lock out this hash table and list.
    mutexes[index].lock();

    MapType& map = hashTables[index];
    ListType& list = lists[index];
 
    double previousTime = 0;
    if (list.size() > 0) {
      previousTime = list.back().second;
    }
    if (currentTime < previousTime) {
      throw TemporalSetException("TemporalSet::insert currentTime < "
        "previousTime"); 
    }

    // First erase edges.  We look in the list first, which should be ordered
    // by time, and then erase corresponding entries in the map.
    auto iter = list.begin();
    for (; iter != list.end();) 
    {
      TimeType t = iter->second;
     
      if (currentTime - t > timeToLive) {
        
        K deleteKey = iter->first;
        map.erase(deleteKey);
        iter = list.erase(iter);
        
      } else {
        ++iter;
      }
    }

    // Add the key/currentTime pair to the end of the list.
    list.push_back(pair);

    // Add the key and it's associated time to the hash table.
    map[key] = currentTime;
    
    mutexes[index].unlock();

    return true;
  }

  /**
   * Returns true if the key is found in the set. 
   */ 
  bool contains(K key)
  {
    size_t index = hashFunction(key) % tableCapacity;
    mutexes[index].lock();
    bool result = hashTables[index].count(key) > 0;
    mutexes[index].unlock();
    return result; 
  }

  /**
   * Returns the number keys in the set.
   */
  size_t size()
  {
    size_t total = 0;
    for (size_t i = 0; i < tableCapacity; i++) 
    {
      total += hashTables[i].size();
    }
    return total;
  }
};
  
} //end namespace sam



#endif
