#ifndef TUPLE_HPP
#define TUPLE_HPP

class Tuple {
public:
  Tuple() {}
  virtual ~Tuple() {}

  virtual std::string getField(size_t field) const = 0;
  virtual std::string toString() const = 0;

};

#endif
