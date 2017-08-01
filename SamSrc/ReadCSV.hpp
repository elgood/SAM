#ifndef SAM_READCSV_HPP
#define SAM_READCSV_HPP

#include "BaseProducer.hpp"
#include "Netflow.hpp"
#include "AbstractDataSource.hpp"

namespace sam {

class ReadCSV : public BaseProducer<Netflow>, public AbstractDataSource
{
  std::string filename;
  std::ifstream file;
public:
  /**
   * \param filename The location of a CSV file.
   */
  ReadCSV(std::string _filename) : BaseProducer<Netflow>(1) {
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
      //std::cout << "line " << line << std::endl;
      // We will use the order they come in as the SamGeneratedId.
      // This assumes that there is a label in each line.
      try {
        Netflow netflow = makeNetflow(i, line);
        for (auto consumer: consumers) {
          consumer->consume(netflow);
        }
      } catch (std::exception e) {
        std::cerr << e.what() << std::endl; 
      }
      i++;
    }
  }

};

}
#endif
