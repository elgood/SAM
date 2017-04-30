#ifndef TUPLE_HPP
#define TUPLE_HPP

class Tuple {
public:
  Tuple() {}
  virtual ~Tuple() {}

  virtual std::string getField(size_t field) const = 0;
  virtual void setField(size_t field, std::string value) const = 0;
  virtual std::string toString() const = 0;

};

template <typename TupleType>
class Tuple {

  
};

#endif
