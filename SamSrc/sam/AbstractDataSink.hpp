#ifndef SAM_ABSTRACT_DATA_SINK
#define SAM_ABSTRACT_DATA_SINK

namespace sam {

/**
 * Abstract class for things that send data to a network endpoint or a file.
 */
class AbstractDataSink
{
  AbstractDataSink() {}
  virtual ~AbstractDataSink() {}

  virtual bool connect() = 0;
};

}

#endif
