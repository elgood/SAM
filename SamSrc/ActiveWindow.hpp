#ifndef ACTIVE_WINDOW_HPP
#define ACTIVE_WINDOW_HPP

#include <map>
#include <algorithm>
#include <iostream>
#include <numeric>

using std::map;

template <typename K>
class ActiveWindow
{
private:
  map<K, size_t> keyCounter; ///> Keeps track of counts for each key.
  size_t count = 0; ///> Total number of elements in this window.
  size_t limit; ///> Max number of elements in the window.

public:

  ActiveWindow(size_t limit) {
    this->limit = limit;
  }

  /**
   * Increments by 1 the count for the given key.
   * \param key The key to add.
   * \return Returns true if count < limit, false otherwise.
   */
  inline bool update(K key) {
    if (count < limit) {
      if (keyCounter.count(key) > 0) {
        size_t value = keyCounter.at(key);
        keyCounter[key] = value + 1;
      } else {
        keyCounter[key] = 1;
      }
      count++;
      return true;
    }
    return false;
  }

  /**
   * Returns the topk elements
   */
  inline std::vector<std::pair<K, size_t>> topk(size_t n) const
  
  {

    // Make a vector of pairs and put all the key-value pairs into it.
    std::vector<std::pair<K, size_t>> pairs;
    for (auto itr = keyCounter.begin(); itr != keyCounter.end(); ++itr)
    pairs.push_back(*itr);
        
    // Sort the vector of pairs 
    std::sort(pairs.begin(), pairs.end(), 
          [=](std::pair<K, size_t>& a, std::pair<K, size_t>& b)
         {
               return a.second > b.second;
         }
    );

    std::vector<std::pair<K, size_t>> rVector;
    for (int i = 0; (i < n) && (i < pairs.size()); i++) {
      rVector.push_back(pairs[i]);
    }

    return rVector;
  }


  inline size_t getNumElements() {
    return std::accumulate(std::begin(keyCounter),
              std::end(keyCounter),
              0,
              [](const size_t previous, const std::pair<K, size_t>& p)
              {
                return previous + p.second; 
              }
           );
  }




};

#endif
