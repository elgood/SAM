#ifndef EXPONENTIAL_HISTOGRAM_HPP
#define EXPONENTIAL_HISTOGRAM_HPP

#include <stdexcept>
#include <string>
#include <boost/lexical_cast.hpp>
#include <iostream>

#include "BaseSlidingWindow.hpp"

namespace sam {

template <typename T>
class ExponentialHistogram: public BaseSlidingWindow<T>
{
public:
  static size_t const MAX_SIZE;

private:
  
  // Determines number of buckets.  If there are k/2 + 2 buckets
  // of the same size (k + 2 buckets if the bucket size equals 1), 
  // the oldest two buckets are combined. 
  size_t k;

  // The number of levels.  The first level has k+2 slots.
  // All other levels have k/2 + 2 slots.  The ith level (starting at 0)
  // has slots that represent 2^i numbers.  
  size_t numLevels;

  // The data structure that holds the data of the sliding window.
  T** data;

  // Points to where data should be added
  size_t* ends;

  // An array of booleans that keeps track of which levels need to be merged
  bool* needToMerge;

  // If all the storage in one level has been used, this is set to true.
  // There is different processing depending on if we have seen the entire level
  // or not.
  bool* onePass;

  T total = 0;

public:
  ExponentialHistogram(size_t N, size_t k) : BaseSlidingWindow<T>(N)
  {
    if (N == 0) {
      throw std::out_of_range("Cannot specify 0 as the size of window");
    }

    if (N >= MAX_SIZE) {
      throw std::out_of_range("Specified N > MAX_SIZE which is " +
                              boost::lexical_cast<std::string>(MAX_SIZE));
    }

    this->k = k;

    // Sets numLevels
    determineNumLevels();  

    data = new T*[numLevels];
    
    // The first level has k + 2 elements
    data[0] = new T[k + 2];
    for (int i = 1; i < numLevels; i++) {
      data[i] = new T[k/2 + 2];  
    }

    onePass = new bool[numLevels];
    for (int i = 0; i < numLevels; i++) onePass[i] = false;

    needToMerge = new bool[numLevels];
    for (int i = 0; i < numLevels; i++) needToMerge[i] = false;

    ends = new size_t[numLevels];
    for (int i = 0; i < numLevels; i++) ends[i] = 0;

  }

  virtual ~ExponentialHistogram() {
    for(int i = 0; i < numLevels; i++) {
      delete[] data[i];
    }
    delete[] data;
    delete[] onePass;
    delete[] ends;
  }

  /**
   * Add the specified item to the window.  If the window is full,
   * the item at the end is dropped.
   */
  void add(T item) {
    //update the global total
    total = total + item;

    // Add the item to the data structure.
    add(item, 0);
  } 

  /**
   * Returns the number of levels.  The ith level represents
   * 2^i items aggregated together.  There are k+2 values in the
   * 0th level, and k/2 + 1 values for levels > 0.
   */
  size_t getNumLevels() {
    return numLevels;
  }

  T getTotal() {
    return total;
  }

  /**
   * Returns the total number of numbers that can be represented by
   * the histogram.
   */
  size_t getNumSlots() {
    int size = 1;
    int total = 0;
    total = size * (k + 2);
    for (int i = 1; i < numLevels; i++) {
      size = size * 2;
      total = total + size * (k/2 + 2); 
    }
    return total;
  }
  
private:

  void add(T item, size_t level) {
    if (level < numLevels) {
      // Going through the level for the first time.  
      // We can just add items without worrying about overwriting values 
      // or the need to merge. 
      if (!onePass[level]) 
      { 
        data[level][ends[level]] = item;
        incrementEnd(level);
        // we passed through the level once
        if (ends[level] == 0) {
          onePass[level] = true;
          needToMerge[level] = true;
        }
      } 
      // We have gone through the level at least once.  We have to worry about
      // writing over values and the need to merge values to send to the level
      // above.
      else {

        // Adding an item will force a merger
        if (needToMerge[level]) { 
          // index of the first item to merge.
          size_t first = data[level][ends[level]]; 
          
          // index of the second item to merge.
          size_t second = data[level][endPlusOne(level)]; 
          
          // Adding merged item to the next level
          add(first + second, level + 1); 
          
          // Adding the new item to the now open space
          data[level][ends[level]] = item; 
          
          // The next addition won't require a merger since we cleared out
          // two spaces.
          needToMerge[level] = false; 

          incrementEnd(level);
        } 
        // Still have space; no merger needed.
        else { 
          data[level][ends[level]] = item;
          incrementEnd(level);
          needToMerge[level] = true;
        }
      }
    }
    // If there isn't another level, we update the total and drop the item.
    else 
    {
      total = total - item;
    }
  }

  /**
   * Returns the index of the end incremented by 1 for the
   * specified level.
   */
  size_t endPlusOne(size_t level) {
    size_t tempEnd = ends[level] + 1;
    if (((level == 0) && (tempEnd >= (k + 1))) ||
        ((level > 0) && (tempEnd >= (k/2 + 1))))
    {
      return 0;
    }
    return tempEnd;
  }

  /**
   * Increments the end index for the specified level.
   */
  void incrementEnd(size_t level) {
    ends[level]++;
    if (((level == 0) && (ends[level] >= (k + 2))) ||
        ((level > 0) && (ends[level] >= (k/2 + 2))))
    {
      ends[level] = 0;
    }
  }

  // Determines the number of bins necessary for the sliding window
  // of size N.
  void determineNumLevels() {
    size_t total = 0;
    numLevels = 1;

    // first level has k + 2 slots, each representing one number
    total = k + 2;
    
    while (total <= this->N) {
      total = total + (k/2 + 2) * pow(2, numLevels);   
      numLevels++;
    }
  }

};

template <typename T>
size_t const ExponentialHistogram<T>::MAX_SIZE = 10000000;

}
#endif
