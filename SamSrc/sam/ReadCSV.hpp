#ifndef SAM_READCSV_HPP
#define SAM_READCSV_HPP

#include <sam/BaseProducer.hpp>
#include <sam/AbstractDataSource.hpp>
#include <fstream>

namespace sam {

template <typename TupleType, typename Tuplizer>
class ReadCSV : public BaseProducer<TupleType>, public AbstractDataSource
{
private:
  std::string filename;
  std::ifstream file;
  Tuplizer tuplizer;
public:
  /**
   * \param filename The location of a CSV file.
   */
  ReadCSV(std::string _filename) : BaseProducer<TupleType>(1) {
    filename = _filename;
  }

  ~ReadCSV() {
    file.close();
  }

  bool connect() {
    file = std::ifstream(filename); 
    return true;
  }
  
  void receive()
  {
    int i = 0;
    std::string line;
    while(std::getline(file, line)) {
      
      // We will use the order they come in as the SamGeneratedId.
      // This assumes that there is a label in each line.
      TupleType tuple = tuplizer(i, line); 
      for (auto consumer: this->consumers) {
        consumer->consume(tuple);
      }
    }
  }

};

}
#endif
