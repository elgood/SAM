#ifndef SAM_READCSV_HPP
#define SAM_READCSV_HPP

#include <sam/BaseProducer.hpp>
#include <sam/AbstractDataSource.hpp>
#include <sam/FeatureProducer.hpp>
#include <sam/tuples/Edge.hpp>
#include <sam/Features.hpp>
#include <fstream>

namespace sam {

template <typename EdgeType, typename Tuplizer>
class ReadCSV : public BaseProducer<EdgeType>, 
  public AbstractDataSource, public FeatureProducer
{
private:
  std::string filename; ///> File to read
  std::ifstream file; ///> File handle
  Tuplizer tuplizer; ///> Tuplizer to convert to edges

  // Generates unique id for each tuple
  SimpleIdGenerator* idGenerator = idGenerator->getInstance(); 
    
public:
  /**
   * \param filename The location of a CSV file.
   */
  ReadCSV(size_t nodeId, std::string _filename) : 
    BaseProducer<EdgeType>(nodeId, 1) 
  {
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
     
      size_t id = idGenerator->generate(); 
      EdgeType edge = tuplizer(id, line); 
      for (auto consumer: this->consumers) {
        consumer->consume(edge);
      }

      this->notifySubscribers(edge.id, std::get<0>(edge.label));
      
      i++;
    }
  }

};

}
#endif
