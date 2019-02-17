#ifndef SLIDING_WINDOW_H
#define SLIDING_WINDOW_H

#include <queue>
#include <map>
#include <vector>
#include <stdexcept>
#include <string>
#include <iostream>

#include <boost/lexical_cast.hpp>

#include <sam/ActiveWindow.hpp>
#include <sam/DormantWindow.hpp>

namespace sam {

template <typename K>
class SlidingWindow
{
private:
  size_t N; ///> The total number of elements in the sliding window
  size_t b; ///> The number elements represented in a block
  size_t k; ///> The number of top elements to keep track of
  size_t counter = 0; ///>how many elements have been processed in active window
  ActiveWindow<K> active; ///> The active window
  std::queue<DormantWindow<K>> queue; ///> All the dormant windows
  std::map<K, size_t> globalInfo; ///> Global counts on frequent keys
  int numDormant; ///> The number of dormant windows (N/b - 1)
  
public:
  SlidingWindow(int N, int b, int k) : active(b)
  {
    this->N = N;
    this->b = b;
    this->k = k;
    numDormant = N / b - 1;
    if (numDormant <= 0) {
      std::string message = "Num dormant was less than or equal to zero " +
                            boost::lexical_cast<string>(numDormant);
      throw std::invalid_argument(message); 
    }
  }

  int getNumDormant() const { return numDormant; }

  /**
   * Adds the given key to the sliding window.
   * \param key
   */
  void add(K key) 
  {
    // If the counter is less than b, we add the key to the active window
    if (counter < b) {
      active.update(key);
      counter++;
    } 
    // When the active window is full
    else {
      // Create a new dormant window based on the current active window
      DormantWindow<K> newDormant(k, active);
      queue.push(newDormant); // Add the dormant window to the queue
      addToGlobal(newDormant); // Update the global stats
      active = ActiveWindow<K>(b); // Create a new active window
      active.update(key);
      counter = 1;
    }
    
    // Check to see if we need to get rid of the oldest dormant window.
    if (queue.size() > numDormant) {
      DormantWindow<K> oldest = queue.front();
      queue.pop();
      removeFromGlobal(oldest);
    }
  }

  size_t getNumActiveElements() {
    return active.getNumElements();
  }

  size_t getNumDormantElements() {
    return queue.size() * b;
  }

  std::pair<K, size_t> getIthElement(size_t i) {

    if (i >= globalInfo.size()) {
      std::string message = "Error in GetIth Element: Size of global info: " + 
        boost::lexical_cast<std::string>(globalInfo.size()) +
        " Requested element " + boost::lexical_cast<std::string>(i);
      throw std::out_of_range(message);
    }

    std::vector<std::pair<K, size_t>> pairs;
    for (auto itr = globalInfo.begin(); itr != globalInfo.end(); ++itr)
      pairs.push_back(*itr);

    std::sort(pairs.begin(), pairs.end(),
     [=](std::pair<K, size_t> const & a, std::pair<K, size_t> const & b)
              {
               return a.second > b.second;
              }
    );

    return pairs[i];
  }

  /**
   * Returns a vector of the keys in string format in descending order.
   */
  std::vector<string> getKeys() {
    std::vector<string> keys;
    int limit = globalInfo.size();  
    for (int i = 0; i < limit; i++) {
      auto p = getIthElement(i);
      string key = boost::lexical_cast<string>(p.first);
      keys.push_back(key);
    }
    return keys;
  }

  std::vector<double> getFrequencies() {
    std::vector<double> frequencies;
    int limit = globalInfo.size();  
    for (int i = 0; i < limit; i++) {
      auto p = getIthElement(i);
      double count = boost::lexical_cast<double>(p.second);
      frequencies.push_back(count);
    }

    double total = static_cast<double>(getNumDormantElements()); 

    std::transform(frequencies.begin(), 
                  frequencies.end(),
                  frequencies.begin(),
                  [total](double &item){ return item / total; });

    return frequencies;
  }

private:

  /**
   * Addes the specified Dormant window's stats to the global stats.
   * \param newDormant
   */
  void addToGlobal(DormantWindow<K> newDormant) 
  {
    int actualK = newDormant.getNumKeys() < k ? newDormant.getNumKeys() : k;
    for (int i = 0; i < actualK; i++) {
      std::pair<K, size_t> t = newDormant.getIthMostFrequent(i);
      K key = t.first;
      size_t value = t.second;
      if (globalInfo.count(key) > 0) {
        globalInfo[key] = globalInfo[key] + value;
      } else {
        globalInfo[key] = value;
      }
    }
  }
  
  /**
   * Removes the specified dormant window's statistics from the 
   * global stats.
   * \param oldest
   */
  void removeFromGlobal(DormantWindow<K> oldest)
  {
    int actualK = oldest.getNumKeys() < k ? oldest.getNumKeys() : k;
    for (int i = 0; i < actualK; i++) {
      std::pair<K, size_t> t = oldest.getIthMostFrequent(i);
      K key = t.first;
      size_t value = t.second;
      if (globalInfo.count(key) > 0) {
        size_t newValue = globalInfo[key] - value;
        globalInfo[key] = newValue;

        if (globalInfo[key] <= 0) {
          globalInfo.erase(key);
        }
      } 
    }
  }

};

}

#endif
