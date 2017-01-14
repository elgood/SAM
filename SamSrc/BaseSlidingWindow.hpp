#ifndef BASE_SLIDING_WINDOW_HPP
#define BASE_SLIDING_WINDOW_HPP

namespace sam {

template <typename T>
class BaseSlidingWindow
{
protected:
  // The number of items in the sliding window
  size_t N;
public:
  BaseSlidingWindow(size_t N) {
    this->N = N;
  }

  virtual ~BaseSlidingWindow() {

  }

  size_t getN() {
    return N;
  }

};

}

#endif
