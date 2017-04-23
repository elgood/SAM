#ifndef TRANSFORM_PRODUCER_HPP
#define TRANSFORM_PRODUCER_HPP

#include <queue>

namespace sam {


template <typename InputType, typename OutputType>
class TransformProducer : public AbstractConsumer<InputType>,
                          public BaseProducer<OutputType>
{
private:
  // How many previous values we need to keep
  size_t historyLength;

  // An fixed-size array that holds a history of the inputs.
  InputType *inputHistory;

  // The index to the current

public:
  TransformProducer(size_t queueLength,
                    size_t historyLength);
  virtual ~TransformProducer();

  virtual bool consume(InputType const& input);

};

template <typename InputType, typename OutputType>
TransformProducer<InputType, OutputType>::TransformProducer(
  size_t queueLength,
  size_t historyLength) 
  : 
  BaseProducer(queueLength)
{
  this->historyLegnth = historyLength;
  inputHistory = new InputType[historyLength];
}

template <typename InputType, typename OutputType>
TransformProducer<InputType, OutputType>::~TransformProducer()
{
  delete[] inputHistory;
}

template <typename InputType, typename OutputType>
TransformProducer<InputType, OutputType>::consume(InputType const& input)
{

}


}
#endif
