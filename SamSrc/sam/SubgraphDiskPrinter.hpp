#ifndef SUBGRAPH_DISK_PRINTER_HPP
#define SUBGRAPH_DISK_PRINTER_HPP

#include <iostream>
#include <fstream>
#include <sam/AbstractSubgraphPrinter.hpp>

namespace sam {

/**
 * A class that prints subgraph query results to disk.
 */
template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
class SubgraphDiskPrinter : 
  public AbstractSubgraphPrinter<TupleType, source, target, time, duration>
{
public:
  typedef typename AbstractSubgraphPrinter<TupleType, source, target, time, 
    duration>::ResultType ResultType;

  SubgraphDiskPrinter(std::string fileLocation)
  {
    try {
      ofile.open(fileLocation);
    } catch (std::exception e) {
      printf("Couldn't open file %s for printing subgraph results\n",
        fileLocation.c_str());
    }
  }

  ~SubgraphDiskPrinter()
  {
    try {
      ofile.close();
    } catch (std::exception e) {
      printf("Couldn't close ofile\n");
    }
  }

  /**
   * Prints the result to disk location specified in constructor.
   * Virtual so that inheriting classes can override the implementation.
   */
  virtual void print(ResultType const& result);

private:
  std::ofstream ofile;
  std::mutex lock;
};

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
void SubgraphDiskPrinter<TupleType, source, target, time, duration>::
  print(ResultType const& result)
{
  lock.lock(); // Only allow one thread to write at a time.
  try {
    ofile << result.toString() << std::endl;
  } catch (std::exception e) {
    DEBUG_PRINT("Troubles writing subgraph result to disk\n");
  }
  lock.unlock();
}

}

#endif
