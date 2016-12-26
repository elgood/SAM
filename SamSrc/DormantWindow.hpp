#ifndef DORMANT_WINDOW_HPP
#define DORMANT_WINDOW_HPP

#include <vector>
#include <stdexcept>
#include <boost/lexical_cast.hpp>
#include "ActiveWindow.hpp"

using std::string;

template <typename K>
class DormantWindow
{
private:
  // How many values are tracked.
  size_t k;

  // TODO Try using bitset (boost or std) to save space  
  std::vector<std::pair<K, size_t>> storage;

public:
  DormantWindow(size_t k, ActiveWindow<K> const & active)
  {
    this->k = k;
    std::vector<std::pair<K, size_t>> sortedList = active.topk(k);
    int actualK = sortedList.size() < k ? sortedList.size() : k;
    for (int i = 0; i < actualK; i++) {
      storage.push_back(sortedList[i]);
    }
  }

  std::pair<K, size_t> getIthMostFrequent(int i) const {
    if (i < k) {
      return storage[i];
    }
    string message = "i: " + boost::lexical_cast<string>(i) +
                     " is not less than k: " + boost::lexical_cast<string>(k);
    throw std::out_of_range(message);
  }

  size_t getNumKeys() const {
    return storage.size();
  }

  size_t getLimit() const {
    return k;
  }

};

#endif
