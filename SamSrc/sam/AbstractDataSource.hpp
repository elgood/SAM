#ifndef SAM_ABSTRACT_DATA_SOURCE
#define SAM_ABSTRACT_DATA_SOURCE

namespace sam {

/**
 * Abstract class for things that get the data from a feed like a socket
 * or a file.
 */
class AbstractDataSource {
public:
  AbstractDataSource() {}
  virtual ~AbstractDataSource() {}

  virtual void receive()  = 0;
  virtual bool connect()  = 0;

};

} /* namespace sam */

#endif
